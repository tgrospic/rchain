package coop.rchain.casper.finality

import cats.effect.{Concurrent, Sync}
import cats.syntax.all._
import cats.{Applicative, Show}
import coop.rchain.casper.safety.CliqueOracle
import coop.rchain.casper.safety.CliqueOracle.WeightMap
import coop.rchain.dag.DagReader
import coop.rchain.metrics.{Metrics, Span}
import coop.rchain.shared.Log
import coop.rchain.shared.syntax._
import fs2.Stream

import scala.reflect.ClassTag

/**
  * Block can be recorded as last finalized block (LFB) if Safety oracle outputs fault tolerance (FT)
  * for this block greater then some predefined threshold. This is defined by [[CliqueOracle.computeMaxCliqueWeight]]
  * function, which requires some target block as input arg.
  *
  * Therefore: Finalizer has a scope of search, defined by tips and previous LFB - each of this blocks can be next LFB.
  *
  * We know that LFB advancement is not necessary continuous, next LFB might not be direct child of current one.
  *
  * Therefore: we cannot start from current LFB children and traverse DAG from the bottom to the top, calculating FT
  * for each block. Also its computationally ineffective,
  *
  * BUT we know that scope of search for potential next LFB is constrained. Block A can be finalized only
  * if it has more then half of total stake in bonds map of A translated from tips throughout main parent chain.
  * IMPORTANT: only main parent relation gives weight to potentially finalized block.
  *
  * Therefore: Finalizer should seek for next LFB going through 2 steps:
  *   1. Find messages M in scope of search that have more then half of the stake translated through main parent chain
  *     from tips down to M.
  *   2. Execute [[CliqueOracle.computeMaxCliqueWeight]] on these targets
  *   3. Candidate with highest block number passing finalization threshold become next LFB
  */
object Finalizer {

  /** message that is agreed on + weight of this agreement */
  final case class Agreement[M, V](message: M, stakeAgreed: WeightMap[V])

  // if more then half of stake agree on message - it is considered safe from orphaning
  def cannotBeOrphaned[F[_]: Applicative, M, V](
      messageWeightMap: WeightMap[V],
      agreeingWeightMap: WeightMap[V]
  ): Boolean = {
    require(
      (agreeingWeightMap.filterNot { case (_, stake) => stake > 0 }.keySet -- messageWeightMap.keySet).isEmpty,
      message = "Agreeing map contains not bonded validators"
    )
    val activeStakeTotal    = messageWeightMap.values.sum
    val activeStakeAgreeing = agreeingWeightMap.values.sum
    // in theory if each stake is high enough, e.g. Long.MaxValue, sum of them might result in negative value
    require(activeStakeTotal > 0, message = "Long overflow when computing total stake")
    require(activeStakeAgreeing > 0, message = "Long overflow when computing total stake")
    activeStakeAgreeing > activeStakeTotal.toFloat / 2
  }

  /** Create an agreement given validator that agrees on a message and weight map of a message */
  def recordAgreement[V](
      messageWeightMap: WeightMap[V],
      agreeingValidator: V
  ): WeightMap[V] = {
    // if validator is not bonded in the message - agreed stake is 0.
    val stakeAgreed = messageWeightMap.getOrElse(agreeingValidator, 0L)
    Map(agreeingValidator -> stakeAgreed)
  }

  /** TODO These are fully identified by message and lastFinalizedHeight, so there might be potential for caching
    *  DAG is not a variable because parents, so DAG structure, - is defined by message. */
  /**
    * Agreements propagated by message to its ancestors
    * @param message message that agrees
    * @param validator creator of a message
    * @param dag message dependency graph
    * @param stopHeight height of message to stop traversing the DAG
    * @param heightF function to get message height
    * @param messageWeightMapF function to get bonds map (weights map) for some message
    * @return Stream of agreements propagated by message onto its ancestors down till stopHeight
    */
  def messageAgreements[F[_]: Sync, M, V](
      message: M,
      validator: V,
      dag: DagReader[F, M],
      stopHeight: Long,
      heightF: M => F[Long],
      messageWeightMapF: M => F[WeightMap[V]]
  ): Stream[F, Agreement[M, V]] =
    Stream
      .unfoldEval[F, M, Agreement[M, V]](message)(
        m =>
          heightF(m).flatMap(
            height =>
              if (height <= stopHeight)
                none[(Agreement[M, V], M)].pure // siblings and parents of LFB are not of interest
              else
                for {
                  messageWeightMap <- messageWeightMapF(m)
                  out              = Agreement(m, recordAgreement(messageWeightMap, validator)) // output agreement
                  r                <- dag.mainParent(m).map(_.map((out, _))) // proceed to main parent
                } yield r
          )
      )

  /** Find highest finalized block. Scope of the search is constrained by height of last finalized message. */
  def run[F[_]: Concurrent: Metrics: Log: Span, M, V: ClassTag](
      dag: DagReader[F, M],
      faultToleranceThreshold: Float,
      lastFinalizedHeight: Long,
      latestMessages: Seq[(V, M)]
  )(
      newLfbEffect: M => F[Unit],
      finalisationEffect: M => F[Unit],
      isFinalised: M => F[Boolean],
      weightMapF: M => F[WeightMap[V]],
      heightF: M => F[Long],
      toJustificationF: M => F[CliqueOracle.Justification[M, V]],
      justificationsF: M => F[Set[CliqueOracle.Justification[M, V]]]
  )(implicit show: Show[M]): F[Option[M]] = {

    final case class FinalizedCandidate(message: M, height: Long, faultTolerance: Float)

    // weight map as per message, look inside [[CliqueOracle.getCorrespondingWeightMap]] description for more info
    val messageWeightMapF = (m: M) => CliqueOracle.getCorrespondingWeightMap(m, dag, weightMapF)

    // stream of agreements passed down from tips to ancestor messages
    val agreements: Stream[F, Agreement[M, V]] = {
      val latestMessageStreams = latestMessages.map {
        case (v, m) =>
          messageAgreements[F, M, V](m, v, dag, lastFinalizedHeight, heightF, messageWeightMapF)
      }
      Stream.emits(latestMessageStreams).parJoinProcBounded
    }

    // aggregate map [message -> agreeing stake]
    val agreementsMap = agreements
      .fold(Map.empty[M, WeightMap[V]]) {
        case (acc, Agreement(message, stakeAgreed)) => {
          val curVal = acc.getOrElse(message, Map.empty)
          val newVal = curVal ++ stakeAgreed
          acc.updated(message, newVal)
        }
      }

    // stream of messages that meet finalization criteria
    val finalizedCandidates = agreementsMap
      .map(m => Stream.fromIterator(m.toIterator))
      .flatten
      .evalFilter {
        case (message, stakeAgreed) =>
          messageWeightMapF(message).map(cannotBeOrphaned(_, stakeAgreed))
      }
      .evalMap {
        case (message, stakeAgreed) =>
          for {
            messageWeightMap <- messageWeightMapF(message)
            height           <- heightF(message)
          } yield (message, stakeAgreed, messageWeightMap, height)

      }
      .parEvalMapProcBounded {
        case (message, stakeAgreed, messageWeightMap, height) => {
          val totalStake = messageWeightMap.values.sum
          val agreeingWeightMap = stakeAgreed.filter {
            case (_, stake) => stake != 0
          }
          val agreeingLatestMessages = latestMessages.filter {
            case (v, _) => stakeAgreed.contains(v)
          }.toMap
          CliqueOracle
            .computeOutput[F, M, V](
              target = message,
              dag = dag,
              totalStake = totalStake,
              agreeingWeightMap = agreeingWeightMap,
              agreeingLatestMessages = agreeingLatestMessages,
              toJustificationF = toJustificationF,
              justificationsF = justificationsF,
              heightF = heightF
            )
            .map(FinalizedCandidate(message, height, _))
        }
      }
      .filter(_.faultTolerance > faultToleranceThreshold)

    val recordNewLfb = (newLfb: M) =>
      for {
        _ <- newLfbEffect(newLfb)
        _ <- dag
              .withAncestors(newLfb, m => isFinalised(m).not)
              .mapF(finalisationEffect)
              .toList
      } yield ()

    def checkValidResult(
        finalizedCandidates: List[FinalizedCandidate]
    ): F[List[FinalizedCandidate]] =
      Concurrent[F]
        .raiseError[List[FinalizedCandidate]](
          new Exception(s"More then one finalized candidate at a particular height.")
        )
        .whenA(finalizedCandidates.map(_.height).distinct.size != finalizedCandidates.size)
        .as(finalizedCandidates)

    for {
      finalizedCandidates <- finalizedCandidates.compile.toList.flatMap(checkValidResult)
      newLfbOpt           = finalizedCandidates.sortBy(_.height).reverse.headOption.map(_.message)
      _                   <- newLfbOpt.traverse(recordNewLfb)
    } yield newLfbOpt
  }
}

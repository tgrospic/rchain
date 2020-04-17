package coop.rchain.casper.engine

import cats.effect.{Concurrent, Sync}
import cats.syntax.all._
import coop.rchain.blockstorage.BlockStore
import coop.rchain.blockstorage.dag.BlockDagStorage
import coop.rchain.blockstorage.deploy.DeployStorage
import coop.rchain.blockstorage.finality.LastFinalizedStorage
import coop.rchain.casper.LastApprovedBlock.LastApprovedBlock
import coop.rchain.casper._
import coop.rchain.casper.syntax._
import coop.rchain.casper.engine.EngineCell._
import coop.rchain.casper.engine.Running.RequestedBlocks
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.ProtoUtil
import coop.rchain.casper.util.comm.CommUtil
import coop.rchain.casper.util.rholang.RuntimeManager
import coop.rchain.catscontrib.Catscontrib._
import coop.rchain.comm.PeerNode
import coop.rchain.comm.rp.Connect.{ConnectionsCell, RPConfAsk}
import coop.rchain.comm.transport.TransportLayer
import coop.rchain.metrics.{Metrics, Span}
import coop.rchain.rspace.Blake2b256Hash
import coop.rchain.rspace.state.RSpaceStateManager
import coop.rchain.shared
import coop.rchain.shared._

/** Node in this state will query peers in the network with [[ApprovedBlockRequest]] message
  * and will wait for the [[ApprovedBlock]] message to arrive. Until then  it will respond with
  * `F[None]` to all other message types.
  * */
class Initializing[F[_]: Sync: Metrics: Span: Concurrent: BlockStore: CommUtil: TransportLayer: ConnectionsCell: RPConfAsk: RequestedBlocks: Log: EventLog: Time: SafetyOracle: LastFinalizedBlockCalculator: LastApprovedBlock: BlockDagStorage: LastFinalizedStorage: EngineCell: RuntimeManager: EventPublisher: SynchronyConstraintChecker: LastFinalizedHeightConstraintChecker: Estimator: DeployStorage: RSpaceStateManager](
    shardId: String,
    finalizationRate: Int,
    validatorId: Option[ValidatorIdentity],
    theInit: F[Unit]
) extends Engine[F] {

  import Engine._

  override def init: F[Unit] = theInit

  override def handle(peer: PeerNode, msg: CasperMessage): F[Unit] = msg match {
    case ab: ApprovedBlock            => onApprovedBlock(peer, ab)
    case br: ApprovedBlockRequest     => sendNoApprovedBlockAvailable(peer, br.identifier)
    case na: NoApprovedBlockAvailable => logNoApprovedBlockAvailable[F](na.nodeIdentifer)
    case LastFinalizedBlock(block)    => onLastFinalizedBlock(peer, block)
    case _ =>
      Log[F].info(s"Initializing unhandled MSG: $msg") <* ().pure
  }

  private def onApprovedBlock(
      sender: PeerNode,
      approvedBlock: ApprovedBlock
  ): F[Unit] = {
    val senderIsBootstrap = RPConfAsk[F].ask.map(_.bootstrap.exists(_ == sender))
    for {
      _       <- Log[F].info("Received ApprovedBlock message.")
      isValid <- senderIsBootstrap &&^ Validate.approvedBlock[F](approvedBlock)
      _ <- if (isValid) {
            for {
              _       <- Log[F].info("Valid ApprovedBlock received!")
              genesis = approvedBlock.candidate.block
              _ <- EventLog[F].publish(
                    shared.Event.ApprovedBlockReceived(
                      PrettyPrinter
                        .buildStringNoLimit(genesis.blockHash)
                    )
                  )
              _ <- insertIntoBlockAndDagStore[F](genesis, approvedBlock)
              _ <- LastApprovedBlock[F].set(approvedBlock)
              // Request last finalized block from bootstrap node
              _ <- CommUtil[F].requestLastFinalizedBlock
            } yield ()
          } else
            Log[F].info("Invalid ApprovedBlock received; refusing to add.")

    } yield ()
  }

  private def onLastFinalizedBlock(sender: PeerNode, block: BlockMessage): F[Unit] = {
    import cats.instances.list._
    val senderIsBootstrap = RPConfAsk[F].ask.map(_.bootstrap.exists(_ == sender))
    for {
      _ <- Log[F].info(
            s"Received LastFinalizedBlock(${PrettyPrinter.buildString(block.blockHash)}) message"
          )
      _ = println(
        s"JUSTIF: ${block.justifications.map(_.latestBlockHash).map(PrettyPrinter.buildString).mkString(", ")}"
      )
      isValid <- senderIsBootstrap
      _ <- if (isValid) {
            for {
              _       <- Log[F].info("Valid LastFinalizedBlock received!")
              genesis = block
              _ <- EventLog[F].publish(
                    shared.Event.ApprovedBlockReceived(
                      PrettyPrinter
                        .buildStringNoLimit(genesis.blockHash)
                    )
                  )
              // TODO: request signed last finalized block as approved block
              // Add last finalized block as ApprovedBlock
              approvedBlock = ApprovedBlock(ApprovedBlockCandidate(block, 0), Nil)
              _             <- insertIntoBlockAndDagStore[F](genesis, approvedBlock)
              _             <- LastApprovedBlock[F].set(approvedBlock)
              // Transition to restore last finalized state
              preStateHash = Blake2b256Hash.fromByteString(ProtoUtil.preStateHash(block))
              // Transition to restore last finalized state
              _ <- transitionToLastFinalizedState(
                    preStateHash,
                    shardId,
                    finalizationRate,
                    validatorId,
                    theInit
                  )
              // Send request for the first store page
              _ <- CommUtil[F].sendStoreItemsRequest(preStateHash, LastFinalizedState.pageSize)
              _ = println(s"Last finalized preStateHash: ${preStateHash}")
              // Write last finalized state root
              importer = RSpaceStateManager[F].importer
              _        <- importer.setRoot(preStateHash)
              // Request parents and justifications
              parentBlocks        = genesis.header.parentsHashList
              justificationBlocks = genesis.justifications.map(_.latestBlockHash)
              blocks              = parentBlocks ++ justificationBlocks
              _                   <- blocks.distinct.traverse(CommUtil[F].sendBlockRequest)
            } yield ()
          } else
            Log[F].info("Invalid LastFinalizedBlock received; refusing to add.")
    } yield ()
  }

}

package coop.rchain.casper.engine

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Concurrent, Sync}
import cats.implicits.none
import cats.syntax.all._
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.BlockStore
import coop.rchain.blockstorage.dag.BlockDagStorage
import coop.rchain.blockstorage.deploy.DeployStorage
import coop.rchain.blockstorage.finality.LastFinalizedStorage
import coop.rchain.casper.LastApprovedBlock.LastApprovedBlock
import coop.rchain.casper._
import coop.rchain.casper.syntax._
import coop.rchain.casper.engine.EngineCell.EngineCell
import coop.rchain.casper.engine.Running.RequestedBlocks
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.comm.CommUtil
import coop.rchain.casper.util.rholang.RuntimeManager
import coop.rchain.comm.PeerNode
import coop.rchain.comm.rp.Connect.{ConnectionsCell, RPConfAsk}
import coop.rchain.comm.transport.TransportLayer
import coop.rchain.metrics.{Metrics, Span}
import coop.rchain.rspace.Blake2b256Hash
import coop.rchain.rspace.state.RSpaceStateManager
import coop.rchain.shared.{EventLog, EventPublisher, Log, Time}
import scodec.bits.ByteVector
import coop.rchain.shared.ByteVectorOps._

object LastFinalizedState {
  val pageSize = 400 // 3000

}

class LastFinalizedState[F[_]: Sync: Metrics: Span: Concurrent: BlockStore: CommUtil: TransportLayer: ConnectionsCell: RPConfAsk: RequestedBlocks: Log: EventLog: Time: SafetyOracle: LastFinalizedBlockCalculator: LastApprovedBlock: BlockDagStorage: LastFinalizedStorage: EngineCell: RuntimeManager: EventPublisher: SynchronyConstraintChecker: LastFinalizedHeightConstraintChecker: Estimator: DeployStorage: RSpaceStateManager](
    blockHash: Blake2b256Hash,
    stateHash: Blake2b256Hash,
    shardId: String,
    finalizationRate: Int,
    validatorId: Option[ValidatorIdentity],
    theInit: F[Unit]
) extends Engine[F] {
  import Engine._
  import Running._
  import LastFinalizedState._

  case class State(
      position: Long,
      lastPath: Seq[(Blake2b256Hash, Option[Byte])]
  )

  private val reqPath = Ref.unsafe(Seq((stateHash, none[Byte])))

  private val semaphore = Semaphore[F](1)

  override def init: F[Unit] = theInit

  override def handle(peer: PeerNode, msg: CasperMessage): F[Unit] = msg match {
    case StoreItemsMessage(startPath, items, lastPath) =>
      for {
        path <- reqPath.get
        _ <- Log[F].info(
              s"Received store items - match: ${path == startPath}, size: ${items.size}, REQ: $path, START: $startPath"
            )
        _ <- Sync[F].whenA(path == startPath) {
              for {
                lock <- semaphore
                _    <- lock.withPermit(onPartialStateReceived(items, lastPath))
              } yield ()
            }
      } yield ()
    case _ => ().pure
  }

  private def onPartialStateReceived(
      items: Seq[(Blake2b256Hash, ByteString)],
      lastPath: Seq[(Blake2b256Hash, Option[Byte])]
  ): F[Unit] =
    for {
      _ <- ().pure
      // Write incoming data
      importer = RSpaceStateManager[F].importer
      _ <- importer
            .setHistoryItems[ByteString](items, x => ByteVector(x.toByteArray).toDirectByteBuffer)

      // Update cursor (current state restore point)
      _ <- reqPath.set(lastPath)

      // Received the whole state, if received items count is less them page size
      _ <- if (items.size >= pageSize)
            // Send request for the next "page" of last finalized state
            CommUtil[F].sendStoreItemsRequest(StoreItemsMessageRequest(lastPath, 0, pageSize))
          // Break the recursion and transition to running state
          else createCasperAndTransitionToRunning
    } yield ()

  private def createCasperAndTransitionToRunning: F[Unit] =
    for {
      maybeApprovedBlock <- LastApprovedBlock[F].get
      _ <- maybeApprovedBlock.fold(
            Log[F].warn("MultiParentCasper instance not created, approved block not available.")
          ) { approvedBlock =>
            val genesis = approvedBlock.candidate.block
            for {
              casper <- MultiParentCasper
                         .hashSetCasper[F](
                           validatorId,
                           genesis,
                           shardId,
                           finalizationRate,
                           skipValidateGenesis = false
                         )
              _ <- Log[F].info("MultiParentCasper instance created.")
              _ <- Engine
                    .transitionToRunning[F](
                      casper,
                      approvedBlock,
                      validatorId,
                      ().pure[F]
                    )
              _ <- CommUtil[F].sendForkChoiceTipRequest
            } yield ()
          }
    } yield ()

}

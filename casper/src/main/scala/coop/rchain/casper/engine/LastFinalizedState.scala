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
import coop.rchain.catscontrib.BooleanF._
import coop.rchain.catscontrib.Catscontrib.ToBooleanF
import coop.rchain.comm.PeerNode
import coop.rchain.comm.rp.Connect.{ConnectionsCell, RPConfAsk}
import coop.rchain.comm.transport.TransportLayer
import coop.rchain.metrics.{Metrics, Span}
import coop.rchain.rspace.Blake2b256Hash
import coop.rchain.rspace.state.RSpaceStateManager
import coop.rchain.shared.{EventLog, EventPublisher, Log, Time}
import scodec.bits.{BitVector, ByteVector}
import coop.rchain.shared.ByteVectorOps._

object LastFinalizedState {
  val pageSize = 500 // 3000

  // Approved block must be available before restoring last finalized state
  final case object ApprovedBlockNotAvailableWhenRestoringLastFinalizedStateError extends Exception
}

class LastFinalizedState[F[_]: Sync: Metrics: Span: Concurrent: BlockStore: CommUtil: TransportLayer: ConnectionsCell: RPConfAsk: RequestedBlocks: Log: EventLog: Time: SafetyOracle: LastFinalizedBlockCalculator: LastApprovedBlock: BlockDagStorage: LastFinalizedStorage: EngineCell: RuntimeManager: EventPublisher: SynchronyConstraintChecker: LastFinalizedHeightConstraintChecker: Estimator: DeployStorage: RSpaceStateManager](
    stateHash: Blake2b256Hash,
    shardId: String,
    finalizationRate: Int,
    validatorId: Option[ValidatorIdentity],
    theInit: F[Unit]
) extends Engine[F] {
  import LastFinalizedState._

  private val reqPath = Ref.unsafe(Seq((stateHash, none[Byte])))

  private val semaphore = Semaphore[F](1)

  override def init: F[Unit] = theInit

  override def handle(peer: PeerNode, msg: CasperMessage): F[Unit] = msg match {
    case StoreItemsMessage(startPath, lastPath, historyItems, dataItems) =>
      for {
        path <- reqPath.get
        _ <- Log[F].info(
              s"Received store items - match: ${path == startPath}, history: ${historyItems.size}, data: ${dataItems.size}, REQ: $path, START: $startPath"
            )
        _ <- Sync[F].whenA(path.nonEmpty && path == startPath) {
              for {
                lock <- semaphore
                _    <- lock.withPermit(onPartialStateReceived(historyItems, dataItems, lastPath))
              } yield ()
            }
      } yield ()
    case HasBlock(hash)  => handleHasBlock(peer, hash)
    case b: BlockMessage => handleBlockMessage(b)
    case _               => ().pure
  }

  private def handleHasBlock(peer: PeerNode, hash: ByteString) =
    Log[F].info(s"HasBlock received ${PrettyPrinter.buildString(hash)}") *>
      BlockStore[F]
        .contains(hash)
        .ifM(
          ().pure,
          TransportLayer[F].sendToPeer(peer, BlockRequestProto(hash))
        )

  private def handleBlockMessage(block: BlockMessage) =
    Log[F].info(s"BlockMessage received ${PrettyPrinter.buildString(block.blockHash)}") *>
      BlockStore[F]
        .contains(block.blockHash)
        .ifM(
          ().pure,
          for {
            maybeApprovedBlock <- LastApprovedBlock[F].get
            approvedBlock <- maybeApprovedBlock.liftTo(
                              ApprovedBlockNotAvailableWhenRestoringLastFinalizedStateError
                            )
            _ <- addBlockToStoreAndDag(block, approvedBlock)
            _ <- RequestedBlocks.remove(block.blockHash)
            _ <- tryTransitionToRunning
          } yield ()
        )

  private def addBlockToStoreAndDag(block: BlockMessage, approvedBlock: ApprovedBlock): F[Unit] =
    for {
      _ <- BlockStore[F].put(block.blockHash, block)
      _ <- BlockDagStorage[F].insert(block, approvedBlock.candidate.block, invalid = false)
    } yield ()

  private def onPartialStateReceived(
      historyItems: Seq[(Blake2b256Hash, ByteString)],
      dataItems: Seq[(Blake2b256Hash, ByteString)],
      lastPath: Seq[(Blake2b256Hash, Option[Byte])]
  ): F[Unit] =
    for {
      importer <- RSpaceStateManager[F].importer.pure

      // Write incoming data
      _ <- importer
            .setHistoryItems[ByteString](
              historyItems,
              x => ByteVector(x.toByteArray).toDirectByteBuffer
            )
      _ <- importer
            .setDataItems[ByteString](
              dataItems,
              x => ByteVector(x.toByteArray).toDirectByteBuffer
            )

      // Update cursor (current state restore point)
      _ <- reqPath.set(lastPath)

      // Received the whole state, if received items count is less them page size
      _ <- if (historyItems.size >= pageSize)
            // Send request for the next "page" of last finalized state
            CommUtil[F].sendStoreItemsRequest(StoreItemsMessageRequest(lastPath, 0, pageSize))
          // Break the recursion and transition to running state
          else reqPath.set(Seq.empty) >> tryTransitionToRunning
    } yield ()

  private def tryTransitionToRunning: F[Unit] = {
    val isStateRestored = reqPath.get.map(_.isEmpty)
    val hasRequested    = RequestedBlocks[F].read.map(_.nonEmpty)
    val makeTransition  = isStateRestored &&^ ~^(hasRequested)
    val log = isStateRestored.map2(hasRequested)((_, _)) >>= {
      case (a, b) => Log[F].info(s"TRANSITION STATE restored: $a, has requested $b")
    }
    val logRequested = for {
      reqs <- RequestedBlocks[F].read
      _    <- Log[F].info(s"TRANSITION STATE requested count: ${reqs.size}")
    } yield ()
    logRequested >> log >> makeTransition.ifM(
      LastApprovedBlock[F].get
        .flatMap(_.liftTo(ApprovedBlockNotAvailableWhenRestoringLastFinalizedStateError)) >>= createCasperAndTransitionToRunning,
      ().pure
    )
  }

  private def createCasperAndTransitionToRunning(approvedBlock: ApprovedBlock): F[Unit] = {
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

}

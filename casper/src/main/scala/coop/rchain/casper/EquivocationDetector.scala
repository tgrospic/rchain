package coop.rchain.casper

import cats.effect.Sync
import cats.{Applicative, Monad}
import cats.syntax.all._
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.dag.{BlockDagRepresentation, BlockDagStorage, EquivocationsTracker}
import coop.rchain.casper.protocol.{BlockMessage, Bond}
import coop.rchain.casper.util.{DagOperations, DoublyLinkedDag, ProtoUtil}
import coop.rchain.casper.util.ProtoUtil._
import coop.rchain.models.BlockHash.BlockHash
import coop.rchain.models.EquivocationRecord.SequenceNumber
import coop.rchain.models.Validator.Validator
import coop.rchain.models._
import coop.rchain.shared.{Cell, Log, LogSource}

object EquivocationDetector {

  implicit private val logSource: LogSource = LogSource(this.getClass)

  def checkEquivocations[F[_]: Monad: Log](
      blockBufferDependencyDag: DoublyLinkedDag[BlockHash],
      block: BlockMessage,
      dag: BlockDagRepresentation[F]
  ): F[ValidBlockProcessing] =
    for {
      maybeLatestMessageOfCreatorHash <- dag.latestMessageHash(block.sender)
      maybeCreatorJustification       = creatorJustificationHash(block)
      isNotEquivocation               = maybeCreatorJustification == maybeLatestMessageOfCreatorHash
      result <- if (isNotEquivocation) {
                 BlockStatus.valid.asRight[BlockError].pure[F]
               } else if (requestedAsDependency(block, blockBufferDependencyDag)) {
                 BlockStatus.admissibleEquivocation.asLeft[ValidBlock].pure[F]
               } else {
                 for {
                   sender <- PrettyPrinter.buildString(block.sender).pure[F]
                   creatorJustificationHash = PrettyPrinter.buildString(
                     maybeCreatorJustification.getOrElse(ByteString.EMPTY)
                   )
                   latestMessageOfCreator = PrettyPrinter.buildString(
                     maybeLatestMessageOfCreatorHash.getOrElse(ByteString.EMPTY)
                   )
                   _ <- Log[F].warn(
                         s"Ignorable equivocation: sender is $sender, creator justification is $creatorJustificationHash, latest message of creator is $latestMessageOfCreator"
                       )
                 } yield BlockStatus.ignorableEquivocation.asLeft[ValidBlock]
               }
    } yield result

  private def requestedAsDependency(
      block: BlockMessage,
      blockBufferDependencyDag: DoublyLinkedDag[BlockHash]
  ): Boolean =
    blockBufferDependencyDag.parentToChildAdjacencyList.contains(block.blockHash)

  private def creatorJustificationHash(block: BlockMessage): Option[BlockHash] =
    for {
      maybeCreatorJustification <- ProtoUtil.creatorJustification(block)
    } yield maybeCreatorJustification.latestBlockHash

  // See summary of algorithm above
  def checkNeglectedEquivocationsWithUpdate[F[_]: Sync: BlockDagStorage](
      block: BlockMessage,
      dag: BlockDagRepresentation[F],
      genesis: BlockMessage
  ): F[ValidBlockProcessing] =
    for {
      neglectedEquivocationDetected <- isNeglectedEquivocationDetectedWithUpdate[F](
                                        block,
                                        dag,
                                        genesis
                                      )
      status = if (neglectedEquivocationDetected) {
        BlockStatus.neglectedEquivocation.asLeft[ValidBlock]
      } else {
        BlockStatus.valid.asRight[BlockError]
      }
    } yield status

  private def isNeglectedEquivocationDetectedWithUpdate[F[_]: Sync: BlockDagStorage](
      block: BlockMessage,
      dag: BlockDagRepresentation[F],
      genesis: BlockMessage
  ): F[Boolean] =
    BlockDagStorage[F].accessEquivocationsTracker { equivocationsTracker =>
      import cats.instances.list._
      for {
        equivocations <- equivocationsTracker.equivocationRecords
        neglectedEquivocationDetected <- equivocations.toList.existsM { equivocationRecord =>
                                          updateEquivocationsTracker[F](
                                            block,
                                            dag,
                                            equivocationRecord,
                                            genesis,
                                            equivocationsTracker
                                          )
                                        }
      } yield neglectedEquivocationDetected
    }

  /**
    * If an equivocation is detected, it is added to the equivocationDetectedBlockHashes, which keeps track
    * of the block hashes that correspond to the blocks from which an equivocation can be justified.
    *
    * @return Whether a neglected equivocation was discovered.
    */
  private def updateEquivocationsTracker[F[_]: Sync](
      block: BlockMessage,
      dag: BlockDagRepresentation[F],
      equivocationRecord: EquivocationRecord,
      genesis: BlockMessage,
      equivocationsTracker: EquivocationsTracker[F]
  ): F[Boolean] =
    for {
      equivocationDiscoveryStatus <- getEquivocationDiscoveryStatus[F](
                                      block,
                                      dag,
                                      equivocationRecord,
                                      genesis
                                    )
      neglectedEquivocationDetected = equivocationDiscoveryStatus match {
        case EquivocationNeglected =>
          true
        case EquivocationDetected =>
          false
        case EquivocationOblivious =>
          false
      }
      _ <- if (equivocationDiscoveryStatus == EquivocationDetected) {
            equivocationsTracker.updateEquivocationRecord(
              equivocationRecord,
              block.blockHash
            )
          } else ().pure[F]
    } yield neglectedEquivocationDetected

  private def getEquivocationDiscoveryStatus[F[_]: Sync](
      block: BlockMessage,
      dag: BlockDagRepresentation[F],
      equivocationRecord: EquivocationRecord,
      genesis: BlockMessage
  ): F[EquivocationDiscoveryStatus] = {
    val equivocatingValidator = equivocationRecord.equivocator
    val latestMessages        = toLatestMessageHashes(block.justifications)
    val maybeEquivocatingValidatorBond =
      bonds(block).find(_.validator == equivocatingValidator)
    maybeEquivocatingValidatorBond match {
      case Some(Bond(_, stake)) =>
        getEquivocationDiscoveryStatusForBondedValidator[F](
          dag,
          equivocationRecord,
          latestMessages,
          stake,
          genesis
        )
      case None =>
        /*
         * Since block has dropped equivocatingValidator from the bonds, it has acknowledged the equivocation.
         * The combination of Validate.transactions and Validate.bondsCache ensure that you can only drop
         * validators through transactions to the proof of stake contract.
         */
        Applicative[F].pure(EquivocationDetected)
    }
  }

  private def getEquivocationDiscoveryStatusForBondedValidator[F[_]: Sync](
      blockDag: BlockDagRepresentation[F],
      equivocationRecord: EquivocationRecord,
      latestMessages: Map[Validator, BlockHash],
      stake: Long,
      genesis: BlockMessage
  ): F[EquivocationDiscoveryStatus] =
    if (stake > 0L) {
      for {
        equivocationDetectable <- isEquivocationDetectable[F](
                                   blockDag,
                                   latestMessages.toSeq,
                                   equivocationRecord,
                                   Set.empty[BlockMetadata],
                                   genesis
                                 )
      } yield
        if (equivocationDetectable) {
          EquivocationNeglected
        } else {
          EquivocationOblivious
        }
    } else {
      // TODO: This case is not necessary if assert(stake > 0) in the PoS contract
      Applicative[F].pure(EquivocationDetected)
    }

  private def isEquivocationDetectable[F[_]: Sync](
      blockDag: BlockDagRepresentation[F],
      latestMessages: Seq[(Validator, BlockHash)],
      equivocationRecord: EquivocationRecord,
      equivocationChildren: Set[BlockMetadata],
      genesis: BlockMessage
  ): F[Boolean] =
    latestMessages match {
      case Nil => false.pure[F]
      case (_, justificationBlockHash) +: remainder =>
        isEquivocationDetectableAfterViewingBlock[F](
          blockDag,
          justificationBlockHash,
          equivocationRecord,
          equivocationChildren,
          remainder,
          genesis
        )
    }

  private def isEquivocationDetectableAfterViewingBlock[F[_]: Sync](
      blockDag: BlockDagRepresentation[F],
      justificationBlockHash: BlockHash,
      equivocationRecord: EquivocationRecord,
      equivocationChildren: Set[BlockMetadata],
      remainder: Seq[(Validator, BlockHash)],
      genesis: BlockMessage
  ): F[Boolean] =
    if (equivocationRecord.equivocationDetectedBlockHashes.contains(justificationBlockHash)) {
      true.pure[F]
    } else {
      for {
        justificationBlock <- blockDag.lookup(justificationBlockHash).map(_.get)
        equivocationDetected <- isEquivocationDetectableThroughChildren[F](
                                 blockDag,
                                 equivocationRecord,
                                 equivocationChildren,
                                 remainder,
                                 justificationBlock,
                                 genesis
                               )
      } yield equivocationDetected
    }

  private def isEquivocationDetectableThroughChildren[F[_]: Sync](
      blockDag: BlockDagRepresentation[F],
      equivocationRecord: EquivocationRecord,
      equivocationChildren: Set[BlockMetadata],
      remainder: Seq[(Validator, BlockHash)],
      justificationBlock: BlockMetadata,
      genesis: BlockMessage
  ): F[Boolean] = {
    val equivocatingValidator = equivocationRecord.equivocator
    val equivocationBaseBlockSeqNum =
      equivocationRecord.equivocationBaseBlockSeqNum
    for {
      updatedEquivocationChildren <- maybeAddEquivocationChild[F](
                                      blockDag,
                                      justificationBlock,
                                      equivocatingValidator,
                                      equivocationBaseBlockSeqNum,
                                      equivocationChildren,
                                      genesis
                                    )
      equivocationDetected <- if (updatedEquivocationChildren.size > 1) {
                               true.pure[F]
                             } else {
                               isEquivocationDetectable[F](
                                 blockDag,
                                 remainder,
                                 equivocationRecord,
                                 updatedEquivocationChildren,
                                 genesis
                               )
                             }
    } yield equivocationDetected
  }

  private def maybeAddEquivocationChild[F[_]: Sync](
      blockDag: BlockDagRepresentation[F],
      justificationBlock: BlockMetadata,
      equivocatingValidator: Validator,
      equivocationBaseBlockSeqNum: SequenceNumber,
      equivocationChildren: Set[BlockMetadata],
      genesis: BlockMessage
  ): F[Set[BlockMetadata]] =
    if (justificationBlock.blockHash == genesis.blockHash) {
      equivocationChildren.pure[F]
    } else if (justificationBlock.sender == equivocatingValidator) {
      // This is a special case as the justificationBlock might be the equivocation child
      if (justificationBlock.seqNum > equivocationBaseBlockSeqNum) {
        addEquivocationChild[F](
          blockDag,
          justificationBlock,
          equivocationBaseBlockSeqNum,
          equivocationChildren
        )
      } else {
        equivocationChildren.pure[F]
      }
    } else {
      // Latest according to the justificationBlock
      val maybeLatestEquivocatingValidatorBlockHash: Option[BlockHash] =
        toLatestMessageHashes(justificationBlock.justifications).get(equivocatingValidator)
      maybeLatestEquivocatingValidatorBlockHash match {
        case Some(blockHash) =>
          for {
            latestEquivocatingValidatorBlock <- blockDag.lookup(blockHash).map(_.get)
            updatedEquivocationChildren <- if (latestEquivocatingValidatorBlock.seqNum > equivocationBaseBlockSeqNum) {
                                            addEquivocationChild[F](
                                              blockDag,
                                              latestEquivocatingValidatorBlock,
                                              equivocationBaseBlockSeqNum,
                                              equivocationChildren
                                            )
                                          } else {
                                            equivocationChildren.pure[F]
                                          }
          } yield updatedEquivocationChildren
        case None =>
          new Exception(
            "justificationBlock is missing justification pointers to equivocatingValidator even though justificationBlock isn't a part of equivocationDetectedBlockHashes for this equivocation record."
          ).raiseError[F, Set[BlockMetadata]]
      }
    }

  private def addEquivocationChild[F[_]: Sync](
      blockDag: BlockDagRepresentation[F],
      justificationBlock: BlockMetadata,
      equivocationBaseBlockSeqNum: SequenceNumber,
      equivocationChildren: Set[BlockMetadata]
  ): F[Set[BlockMetadata]] =
    findCreatorJustificationAncestorWithSeqNum[F](
      blockDag,
      justificationBlock,
      equivocationBaseBlockSeqNum + 1
    ).flatMap {
      case Some(equivocationChildHash) =>
        for {
          equivocationChild <- blockDag.lookup(equivocationChildHash).map(_.get)
        } yield equivocationChildren + equivocationChild
      case None =>
        new Exception(
          "creator justification ancestor with lower sequence number hasn't been added to the blockDAG yet."
        ).raiseError[F, Set[BlockMetadata]]
    }

  private def findCreatorJustificationAncestorWithSeqNum[F[_]: Monad](
      blockDag: BlockDagRepresentation[F],
      b: BlockMetadata,
      seqNum: SequenceNumber
  ): F[Option[BlockHash]] =
    if (b.seqNum == seqNum) {
      Option(b.blockHash).pure[F]
    } else {
      DagOperations
        .bfTraverseF(List(b.blockHash)) { blockHash =>
          getCreatorJustificationAsListUntilGoalInMemory[F](blockDag, blockHash)
        }
        .findF { blockHash =>
          for {
            blockMeta <- blockDag.lookup(blockHash)
          } yield blockMeta.get.seqNum == seqNum
        }
    }
}

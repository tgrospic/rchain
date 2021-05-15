package coop.rchain.rspace.history

import cats.Parallel
import cats.effect.{Concurrent, Sync}
import cats.syntax.all._
import com.typesafe.scalalogging.Logger
import coop.rchain.metrics.{Metrics, Span}
import coop.rchain.rspace._
import coop.rchain.rspace.hashing.{Blake2b256Hash, ChannelHash}
import coop.rchain.rspace.hashing.ChannelHash.{
  hashContinuationsChannels,
  hashDataChannel,
  hashJoinsChannel
}
import coop.rchain.rspace.history.ColdStoreInstances.ColdKeyValueStore
import coop.rchain.rspace.history.instances.RSpaceHistoryReaderImpl
import coop.rchain.rspace.merger.StateMerger
import coop.rchain.rspace.merger.instances.DiffStateMerger
import coop.rchain.rspace.serializers.ScodecSerialize._
import coop.rchain.rspace.state.{RSpaceExporter, RSpaceImporter}
import coop.rchain.shared.syntax._
import coop.rchain.shared.{Log, Serialize}
import fs2.Stream

final case class HistoryRepositoryImpl[F[_]: Concurrent: Parallel: Log: Span, P, A, K](
    currentHistory: History[F],
    rootsRepository: RootRepository[F],
    leafStore: ColdKeyValueStore[F],
    rspaceExporter: RSpaceExporter[F],
    rspaceImporter: RSpaceImporter[F],
    serializeP: Serialize[P],
    serializeA: Serialize[A],
    serializeK: Serialize[K]
) extends HistoryRepository[F, Channel, P, A, K] {

  implicit val ms = Metrics.Source(RSpaceMetricsSource, "history")

  type CacheAction = Blake2b256Hash => F[Unit]
  type ColdAction  = (Blake2b256Hash, Option[PersistedData])

  type Result = (ColdAction, HistoryAction)

  protected[this] val dataLogger: Logger =
    Logger("coop.rchain.rspace.datametrics")

  private def measure(actions: List[HotStoreAction]): F[Unit] =
    Sync[F].delay(
      dataLogger.whenDebugEnabled {
        computeMeasure(actions).foreach(p => dataLogger.debug(p))
      }
    )

  private def computeMeasure(actions: List[HotStoreAction]): List[String] =
    actions.par.map {
      case i: InsertData[A] =>
        val key  = i.channel.hash.bytes
        val data = encodeDatums(i.data)(serializeA)
        s"${key.toHex};insert-data;${data.length};${i.data.length}"
      case i: InsertContinuations[P, K] =>
        val key  = ChannelHash.hashContinuationsChannels(i.channels.map(_.hash)).bytes
        val data = encodeContinuations(i.continuations)(serializeP, serializeK)
        s"${key.toHex};insert-continuation;${data.length};${i.continuations.length}"
      case i: InsertJoins =>
        val key  = i.channel.hash.bytes
        val data = encodeJoins(i.joins)
        s"${key.toHex};insert-join;${data.length}"
      case d: DeleteData =>
        val key = d.channel.hash.bytes
        s"${key.toHex};delete-data;0"
      case d: DeleteContinuations =>
        val key = ChannelHash.hashContinuationsChannels(d.channels.map(_.hash)).bytes
        s"${key.toHex};delete-continuation;0"
      case d: DeleteJoins =>
        val key = d.channel.hash.bytes
        s"${key.toHex};delete-join;0"
    }.toList

  private def calculateStorageActions(action: HotStoreTrieAction): Result = {
    import HistoryRepositoryInstances.{PREFIX_DATUM, PREFIX_JOINS, PREFIX_KONT}

    action match {
      case i: TrieInsertProduce[A] =>
        val data     = encodeDatums(i.data)(serializeA)
        val dataLeaf = DataLeaf(data)
        val dataHash = Blake2b256Hash.create(data)
        (
          (dataHash, Some(dataLeaf)),
          InsertAction(PREFIX_DATUM +: i.hash.bytes.toSeq.toList, dataHash)
        )
      case i: TrieInsertConsume[P, K] =>
        val data              = encodeContinuations(i.continuations)(serializeP, serializeK)
        val continuationsLeaf = ContinuationsLeaf(data)
        val continuationsHash = Blake2b256Hash.create(data)
        (
          (continuationsHash, Some(continuationsLeaf)),
          InsertAction(PREFIX_KONT +: i.hash.bytes.toSeq.toList, continuationsHash)
        )
      case i: TrieInsertJoins =>
        val data      = encodeJoins(i.joins)
        val joinsLeaf = JoinsLeaf(data)
        val joinsHash = Blake2b256Hash.create(data)
        (
          (joinsHash, Some(joinsLeaf)),
          InsertAction(PREFIX_JOINS +: i.hash.bytes.toSeq.toList, joinsHash)
        )
      case d: TrieDeleteProduce =>
        ((d.hash, None), DeleteAction(PREFIX_DATUM +: d.hash.bytes.toSeq.toList))
      case d: TrieDeleteConsume =>
        ((d.hash, None), DeleteAction(PREFIX_KONT +: d.hash.bytes.toSeq.toList))
      case d: TrieDeleteJoins =>
        ((d.hash, None), DeleteAction(PREFIX_JOINS +: d.hash.bytes.toSeq.toList))
    }
  }

  private def transform(hotStoreAction: HotStoreAction): HotStoreTrieAction =
    hotStoreAction match {
      case i: InsertData[A] =>
        val key = i.channel.hash
        TrieInsertProduce(key, i.data)
      case i: InsertContinuations[P, K] =>
        val key = ChannelHash.hashContinuationsChannels(i.channels.map(_.hash))
        TrieInsertConsume(key, i.continuations)
      case i: InsertJoins =>
        val key = i.channel.hash
        TrieInsertJoins(key, i.joins)
      case d: DeleteData =>
        val key = d.channel.hash
        TrieDeleteProduce(key)
      case d: DeleteContinuations =>
        val key = ChannelHash.hashContinuationsChannels(d.channels.map(_.hash))
        TrieDeleteConsume(key)
      case d: DeleteJoins =>
        val key = d.channel.hash
        TrieDeleteJoins(key)
    }

  // this method is what chackpoint is supposed to do, but checkoint operates on actions on channels, and this
  // address by hashes. TODO elaborate unified API
  def doCheckpoint(
      trieActions: Seq[HotStoreTrieAction]
  ): F[HistoryRepository[F, Channel, P, A, K]] = {
    val storageActions = trieActions.par.map(calculateStorageActions)
    val coldActions    = storageActions.map(_._1).collect { case (key, Some(data)) => (key, data) }
    val historyActions = storageActions.map(_._2)

    // save new root for state after checkpoint
    val storeRoot = (root: Blake2b256Hash) => Stream.eval(rootsRepository.commit(root))

    // store cold data
    val storeLeaves =
      Stream.eval(leafStore.putIfAbsent(coldActions.toList).map(_.asLeft[History[F]]))
    // store everything related to history (history data, new root and populate cache for new root)
    val storeHistory = Stream
      .eval(
        history
          .process(historyActions.toList)
          .flatMap(
            resultHistory =>
              storeRoot(resultHistory.root).compile.drain.as(resultHistory.asRight[Unit])
          )
      )

    for {
      newHistory <- Stream
                     .emits(List(storeLeaves, storeHistory))
                     .parJoinProcBounded
                     .collect { case Right(history) => history }
                     .compile
                     .lastOrError
    } yield this.copy[F, P, A, K](currentHistory = newHistory)
  }

  override def checkpoint(
      actions: List[HotStoreAction]
  ): F[HistoryRepository[F, Channel, P, A, K]] = {
    val trieActions = actions.par.map(transform).toList
    for {
      r <- doCheckpoint(trieActions)
      _ <- measure(actions)
    } yield r
  }

  override def reset(root: Blake2b256Hash): F[HistoryRepository[F, Channel, P, A, K]] =
    for {
      _    <- rootsRepository.validateAndSetCurrentRoot(root)
      next = history.reset(root = root)
    } yield this.copy[F, P, A, K](currentHistory = next)

  override def exporter: F[RSpaceExporter[F]] = Sync[F].delay(rspaceExporter)

  override def importer: F[RSpaceImporter[F]] = Sync[F].delay(rspaceImporter)

  override def stateMerger: StateMerger[F] =
    DiffStateMerger[F, P, A, K](historyRepo = this)

  override def history: History[F] = currentHistory

  override def root: Blake2b256Hash = currentHistory.root

  override def getHistoryReader(
      stateHash: Blake2b256Hash
  ): HistoryReader[F, Blake2b256Hash, P, A, K] =
    new RSpaceHistoryReaderImpl(history.reset(root = stateHash), leafStore)(
      Concurrent[F],
      serializeP,
      serializeA,
      serializeK
    )
}

package coop.rchain.state

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}

import cats.Monad
import cats.effect.{Concurrent, Sync}
import cats.syntax.all._
import coop.rchain.blockstorage.FileLMDBIndexBlockStore
import coop.rchain.blockstorage.dag.BlockMetadataPersistentIndex
import coop.rchain.blockstorage.dag.codecs._
import coop.rchain.blockstorage.util.io.IOError.RaiseIOError
import coop.rchain.blockstorage.util.io._
import coop.rchain.casper.protocol.{BlockMessage, BlockMessageProto}
import coop.rchain.casper.storage.RNodeKeyValueStoreManager
import coop.rchain.lmdb.LMDBStore
import coop.rchain.metrics.Metrics
import coop.rchain.rspace.{Blake2b256Hash, Context}
import coop.rchain.shared
import coop.rchain.shared.AttemptOps._
import coop.rchain.shared.ByteStringOps.RichByteString
import coop.rchain.shared.ByteVectorOps.RichByteVector
import coop.rchain.shared.Compression.factory
import coop.rchain.store.KeyValueStore
import monix.eval.Task
import net.jpountz.lz4.{LZ4CompressorWithLength, LZ4DecompressorWithLength}
import org.lmdbjava.DbiFlags.MDB_CREATE
import org.lmdbjava.{Env, EnvFlags}
import org.scalatest._
import scodec.bits.ByteVector
import sun.nio.ch.Util

import scala.concurrent.duration.{Duration, FiniteDuration}

class BlockStoreMigrationSpec extends FlatSpecLike with Matchers {

  it should "copy block metadata index to LMDB" in {
    import monix.execution.Scheduler.Implicits.global

    import scala.concurrent.duration._

    copyMetaData[Task].runSyncUnsafe(2.minutes)
  }

  it should "copy block store to LMDB" in {
    import monix.execution.Scheduler.Implicits.global

    import scala.concurrent.duration._

    copyBlockStore[Task].runSyncUnsafe(30.minutes)
  }

//  val compressor = new LZ4CompressorWithLength(factory.highCompressor(17))
  val compressor   = new LZ4CompressorWithLength(factory.fastCompressor())
  val deCompressor = new LZ4DecompressorWithLength(factory.fastDecompressor())

  def zip(bytes: Array[Byte]): Array[Byte] =
    compressor.compress(bytes)

  def unZip(bytes: Array[Byte]): Array[Byte] =
    deCompressor.decompress(bytes)

  def copyBlockStore[F[_]: Concurrent] = {
    implicit val log                           = new shared.Log.NOPLog[F]()
    implicit val metrics                       = new Metrics.MetricsNOP[F]
    implicit val raiseIOError: RaiseIOError[F] = IOError.raiseIOErrorThroughSync[F]

    val dbDir = Paths.get("/home/tomi/projects/rchain/rchain-test-data/testing-block-store")

    val env    = Context.env(dbDir, 10L * 1024L * 1024L * 1024L)
    val storeF = FileLMDBIndexBlockStore.create[F](env, dbDir).map(_.right.get)

    import cats.instances.vector._

    def toBuffer(bytes: Array[Byte]): ByteBuffer = {
      val buffer: ByteBuffer = Util.getTemporaryDirectBuffer(bytes.length)
//      val buffer: ByteBuffer = ByteBuffer.allocateDirect(bytes.length)
      buffer.put(bytes)
      buffer.flip()
      buffer
    }

    def toZipBuffer(bytes: Array[Byte]): ByteBuffer = {
      val zipBytes = zip(bytes)

//      val unZipped = unZip(zipBytes)
//      val block    = BlockMessageProto.parseFrom(unZipped)
//      println(s"Block unzipped ${Base16.encode(block.blockHash.toByteArray)}")

      val buffer: ByteBuffer = Util.getTemporaryDirectBuffer(zipBytes.length)
//      val buffer: ByteBuffer = ByteBuffer.allocateDirect(zipBytes.length)
      buffer.put(zipBytes)
      buffer.flip()
      buffer
    }

    def bufferToZipBuffer(bufSrc: ByteBuffer): ByteBuffer = {
      val buffer: ByteBuffer = Util.getTemporaryDirectBuffer(bufSrc.limit)
//      val buffer: ByteBuffer = ByteBuffer.allocateDirect(bufSrc.limit)
      compressor.compress(bufSrc, buffer)
      buffer.flip()
      buffer
    }

    def msgToBuffer(block: BlockMessage): ByteBuffer =
      toBuffer(block.toProto.toByteArray)

    def checkKeyValueStore(kv: KeyValueStore[F], isCompressed: Boolean, tag: String) =
      kv.iterate { i =>
        i.map {
            case (k, v) =>
              // Key
              val key = Blake2b256Hash.fromByteArray(ByteVector(k).toArray)
              // Value
              val blockBytesRaw = ByteVector(v).toArray
              val blockBytes    = if (isCompressed) unZip(blockBytesRaw) else blockBytesRaw
              val blockProto    = BlockMessageProto.parseFrom(blockBytes)
              val block         = BlockMessage.from(blockProto).right.get
              // Print
              val blockKey    = key.bytes.toHex
              val blockNUmber = block.body.state.blockNumber
              val blockHash   = ByteVector(block.blockHash.toByteArray).toHex
              println(s"$tag Key: $blockKey, Nr: $blockNUmber, Block: $blockHash")
          }
          .take(5)
          .toVector
          .pure[F]
      }

    for {
      storeOld  <- storeF
      allHashes <- storeOld.iterateIndex(_.toVector)
      _         <- Sync[F].delay(println(s"Items ${allHashes.size}"))

      kvm       <- RNodeKeyValueStoreManager(dbDir)
      storeCopy <- kvm.database("block-storage")

      workingSize = 150000
      readFile = allHashes.take(workingSize).traverse_ {
        case (hash, offset) =>
//          for {
//          // Uses direct and mapped buffer (?)
////            buffer <- storeOld.readByteBuffer(offset)
//            bytes     <- storeOld.readSlice(offset)
//            blockHash = hash.toDirectByteBuffer
//            _         <- store.put(Seq((blockHash, bytes)), toBuffer)
////            _ <- store.put[ByteBuffer](Seq((blockHash, buffer)), identity)
//          } yield ()
          storeOld.get(hash) >>= { maybeBlock =>
            val block     = maybeBlock.get
            val blockHash = hash.toDirectByteBuffer
//            val keyHex       = ByteVector.view(hash.toByteArray).toHex
//            val blockHashHex = ByteVector.view(block.blockHash.toByteArray).toHex
//            val blockNUmber  = block.body.state.blockNumber
//            println(s"READ Key: $keyHex, Nr: $blockNUmber, Block: $blockHashHex")
            storeCopy.put(Seq((blockHash, block)), msgToBuffer)
          }
      }
//      _ <- Lib.time("Read from file storage")(readFile)
      // Check copy DB
      _ <- checkKeyValueStore(storeCopy, false, "COPY")

      // Make another copy from copy
      storeCopy2 <- kvm.database("block-storage-dest")
      srcKeys    <- storeCopy.iterate(_.take(workingSize).map(x => ByteVector(x._1)).toVector)
      // Save test
      copyToCopy2 = srcKeys.grouped(200).toVector.traverse { keys =>
        val keysBuf = keys.map(_.toDirectByteBuffer)
        for {
//          values <- store.get(keysBuf, bufferToZipBuffer)
          values <- storeCopy.get(keysBuf, ByteVector(_))
          kv     = keysBuf.zip(values).map { case (k, v) => (k, v.get) }
//          _  <- storeDest.put[ByteBuffer](kv, identity)
          _ <- storeCopy2.put[ByteVector](kv, x => toZipBuffer(x.toArray))
//          _ <- storeCopy2.put[ByteVector](kv, x => toBuffer(x.toArray))
        } yield ()
      }
      _ <- Lib.time("Copy to copy 2")(copyToCopy2)
      // Check copy 2 DB
      _ <- checkKeyValueStore(storeCopy2, true, "COPY_2")

    } yield ()

  }

  def copyMetaData[F[_]: Sync]() = {
    implicit val log                           = new shared.Log.NOPLog[F]()
    implicit val raiseIOError: RaiseIOError[F] = IOError.raiseIOErrorThroughSync[F]

    val rootPath = Paths.get("/home/tomi/projects/rchain/rchain-test-data/testing-dag-store")
    val logPath  = rootPath.resolve("blockMetadataLogPath")
    val crcPath  = rootPath.resolve("blockMetadataCrcPath")
    val lmdbPath = rootPath.resolve("blockMetadata")

    for {
      // Load file based block metadata index
      blockMetadataIndex <- Lib.time("Load metadata all")(
                             BlockMetadataPersistentIndex.load[F](logPath, crcPath)
                           )
      // Create LMDB store
      _ <- Sync[F].delay(Files.createDirectories(lmdbPath))
      env <- Sync[F].delay {
              val flags = if (true) List(EnvFlags.MDB_NOTLS) else List.empty
              Env
                .create()
                .setMapSize(1024L * 1024L * 1024L)
                .setMaxDbs(1)
                .setMaxReaders(126)
                .open(lmdbPath.toFile, flags: _*)
            }
      dbi <- Sync[F].delay {
              env.openDbi(s"blocks-metadata", MDB_CREATE)
            }
      store = LMDBStore[F](env, dbi)

      // Copy to LMDB
      blockMetadataMap <- blockMetadataIndex.blockMetadataData
      blockMetadataSeq = blockMetadataMap.toSeq.map {
        case (hash, meta) =>
          val hashByteBuffer = codecBlockHash.encode(hash).get.toByteVector.toDirectByteBuffer
          val metaByteBuffer = codecBlockMetadata.encode(meta).get.toByteVector.toDirectByteBuffer
          (hashByteBuffer, metaByteBuffer)
      }
      _ <- Lib.time("Store all")(store.put(blockMetadataSeq.toList))

      _ = println(s"Items ${blockMetadataSeq.size}")
    } yield ()
  }

  object Lib {
    def showTime(d: FiniteDuration) = {
      val ns   = 1d
      val ms   = 1e6 * ns
      val sec  = 1000 * ms
      val min  = 60 * sec
      val hour = 60 * min
      val m    = d.toNanos
      if (m >= hour) s"${m / hour} hour"
      else if (m >= min) s"${m / min} min"
      else if (m >= sec) s"${m / sec} sec"
      else if (m >= ms) s"${m / ms} ms"
      else s"${m / 1e6d} ms"
    }

    def time[F[_]: Monad, A](tag: String)(block: => F[A]): F[A] = {
      val t0 = System.nanoTime
      for {
        a  <- block
        t1 = System.nanoTime
        m  = Duration.fromNanos(t1 - t0)
        _  = println(s">>> $tag elapsed: ${showTime(m)}")
      } yield a
    }
  }
}

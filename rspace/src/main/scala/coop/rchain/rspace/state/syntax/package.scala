package coop.rchain.rspace.state

import java.nio.ByteBuffer
import java.nio.file.Path

import cats.Parallel
import cats.effect.{Concurrent, Sync}
import coop.rchain.rspace.Blake2b256Hash
import coop.rchain.rspace.state.exporters.RSpaceExporterItems.StoreItems
import coop.rchain.rspace.state.exporters.{RSpaceExporterDisk, RSpaceExporterItems}

package object syntax {
  implicit final class RSpaceExporterExt[F[_]](
      // RSpaceExporter extensions / syntax
      private val exporter: RSpaceExporter[F]
  ) {
    // Base export operations

    def getHistory[Value](
        startPath: Seq[(Blake2b256Hash, Option[Byte])],
        skip: Int,
        take: Int,
        fromBuffer: ByteBuffer => Value
    )(implicit m: Sync[F]): F[StoreItems[Blake2b256Hash, Value]] =
      RSpaceExporterItems.getHistory(exporter, startPath, skip, take, fromBuffer)

    def getData[Value](
        startPath: Seq[(Blake2b256Hash, Option[Byte])],
        skip: Int,
        take: Int,
        fromBuffer: ByteBuffer => Value
    )(implicit m: Sync[F]): F[StoreItems[Blake2b256Hash, Value]] =
      RSpaceExporterItems.getData(exporter, startPath, skip, take, fromBuffer)

    // Export to disk

    def writeToDisk(root: Blake2b256Hash, dirPath: Path, chunkSize: Int)(
        implicit m: Concurrent[F],
        p: Parallel[F]
    ): F[Unit] =
      RSpaceExporterDisk.writeToDisk(exporter, root, dirPath, chunkSize)
  }
}

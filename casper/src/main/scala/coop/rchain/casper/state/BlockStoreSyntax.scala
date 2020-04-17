package coop.rchain.casper.state

import cats.Monad
import cats.syntax.all._
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.BlockStore

trait BlockStoreSyntax {
  implicit final def casperSyntaxBlockStore[F[_]](blockStore: BlockStore[F]): BlockStoreOps[F] =
    new BlockStoreOps[F](blockStore)
}

final class BlockStoreOps[F[_]](
    // BlockStore extensions / syntax
    private val blockStore: BlockStore[F]
) {
  def isGenesis(hash: ByteString)(implicit m: Monad[F]) =
    for {
      genesis <- blockStore.getApprovedBlock
    } yield genesis.exists(_.candidate.block.blockHash == hash)
}

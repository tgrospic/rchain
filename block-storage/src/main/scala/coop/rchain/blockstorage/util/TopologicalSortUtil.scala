package coop.rchain.blockstorage.util

import coop.rchain.crypto.codec.Base16
import coop.rchain.models.BlockHash.BlockHash
import coop.rchain.models.BlockMetadata

object TopologicalSortUtil {
  type BlockSort = Vector[Vector[BlockHash]]
  def update(sort: BlockSort, offset: Long, block: BlockMetadata): BlockSort = {
    val hash             = block.blockHash
    val offsetDiff: Long = block.blockNum - offset

    assert(offsetDiff <= Int.MaxValue)
    val number = offsetDiff.toInt

    println(
      s"TopologicalSortUtil.update sortSize: ${sort.size}, offset: $offset, block: ${block.blockNum}, offsetDiff: $offsetDiff"
    )
    println(
      s"  block hash: ${Base16.encode(block.blockHash.toByteArray).take(6)}"
    )

    //block numbers must be sequential, so a new block can only be
    //at a known height or 1 greater than a known height
    assert(number <= sort.length)

    number match {
      //this is a new block height
      case n if n == sort.length => sort :+ Vector(hash)

      //this is another block at a known height
      case n if n < sort.length =>
        val curr = sort(number)
        sort.updated(number, curr :+ hash)
    }
  }
}

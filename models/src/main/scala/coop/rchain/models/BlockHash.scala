package coop.rchain.models

import com.google.protobuf.ByteString
import coop.rchain.crypto.codec.Base16

object BlockHash {
  type BlockHash = ByteString

  val Length = 32
}

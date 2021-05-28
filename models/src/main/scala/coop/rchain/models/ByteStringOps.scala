package coop.rchain.models

import cats.Show
import com.google.protobuf.ByteString
import coop.rchain.crypto.codec.Base16

object ByteStringOps {

  implicit def ordering: Ordering[ByteString] =
    Ordering.by((b: ByteString) => b.toByteArray.toIterable)

  implicit val show = new Show[ByteString] {
    def show(validator: ByteString): String = Base16.encode(validator.toByteArray)
  }

  implicit class ByteStringOps(bs: ByteString) {
    def base16String: String = Base16.encode(bs.toByteArray)
  }
}

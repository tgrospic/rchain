package coop.rchain.crypto.util

import coop.rchain.crypto.PublicKey

object Sorting {

  implicit val byteArrayOrdering = Ordering.by[Array[Byte], Iterable[Byte]](_.toIterable)

  implicit val publicKeyOrdering: Ordering[PublicKey] = Ordering.by[PublicKey, Array[Byte]](_.bytes)

}

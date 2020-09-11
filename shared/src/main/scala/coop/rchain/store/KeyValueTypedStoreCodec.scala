package coop.rchain.store

import cats.effect.Sync
import cats.syntax.all._
import coop.rchain.shared.ByteVectorOps.RichByteVector
import scodec.Codec
import scodec.bits.ByteVector

class KeyValueTypedStoreCodec[F[_]: Sync, K, V](
    store: KeyValueStore[F],
    kCodec: Codec[K],
    vCodec: Codec[V]
) extends KeyValueTypedStore[F, K, V] {
  // TODO: create specialized exceptions for Codec errors
  def encodeKey(key: K): F[ByteVector] =
    kCodec
      .encode(key)
      .fold(
        err => new Exception(err.message).raiseError[F, ByteVector],
        _.toByteVector.pure[F]
      )

  def decodeKey(bytes: ByteVector): F[K] =
    kCodec
      .decodeValue(bytes.toBitVector)
      .fold(
        err => new Exception(err.message).raiseError[F, K],
        _.pure[F]
      )

  def encodeValue(value: V): F[ByteVector] =
    vCodec
      .encode(value)
      .fold(
        err => new Exception(err.message).raiseError[F, ByteVector],
        _.toByteVector.pure[F]
      )

  def decodeValue(bytes: ByteVector): F[V] =
    vCodec
      .decodeValue(bytes.toBitVector)
      .fold(
        err => new Exception(err.message).raiseError[F, V],
        _.pure[F]
      )

  import cats.instances.option._
  import cats.instances.vector._

  override def get(keys: Seq[K]): F[Seq[Option[V]]] =
    for {
      keysBitVector <- keys.toVector.traverse(encodeKey)
      keysBuf       = keysBitVector.map(_.toDirectByteBuffer)
      valuesBytes   <- store.get(keysBuf, ByteVector.view)
      values        <- valuesBytes.toVector.traverse(_.traverse(decodeValue))
    } yield values

  override def put(kvPairs: Seq[(K, V)]): F[Unit] =
    for {
      pairsBitVector <- kvPairs.toVector.traverse {
                         case (k, v) => encodeKey(k).map2(encodeValue(v))((x, y) => (x, y))
                       }
      pairs = pairsBitVector.map { case (k, v) => (k.toDirectByteBuffer, v) }
      _     <- store.put[ByteVector](pairs, _.toDirectByteBuffer)
    } yield ()

  override def delete(keys: Seq[K]): F[Int] =
    for {
      keysBitVector <- keys.toVector.traverse(encodeKey)
      keysBuf       = keysBitVector.map(_.toDirectByteBuffer)
      deletedCount  <- store.delete(keysBuf)
    } yield deletedCount

  override def contains(keys: Seq[K]): F[Seq[Boolean]] =
    for {
      keysBitVector <- keys.toVector.traverse(encodeKey)
      keysBuf       = keysBitVector.map(_.toDirectByteBuffer)
      results       <- store.get(keysBuf, _ => ())
    } yield results.map(_.nonEmpty)

  override def toMap: F[Map[K, V]] =
    for {
      valuesBytes <- store.iterate(
                      _.map { case (k, v) => (ByteVector.view(k), ByteVector.view(v)) }.toVector
                    )
      values <- valuesBytes.traverse {
                 case (k, v) =>
                   for {
                     key   <- decodeKey(k)
                     value <- decodeValue(v)
                   } yield (key, value)

               }
    } yield values.toMap
}

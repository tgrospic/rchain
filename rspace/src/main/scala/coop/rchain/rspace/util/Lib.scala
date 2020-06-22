package coop.rchain.rspace.util

import cats.Monad
import cats.syntax.all._

import scala.concurrent.duration.{Duration, FiniteDuration}

// TEMP: helper functions
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

  def toHex(b: Option[Byte]) =
    b.map(v => String.format("%02x", Integer.valueOf(v & 0xff))).getOrElse("--")
}

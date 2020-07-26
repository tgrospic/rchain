package coop.rchain.rholang.interpreter

import cats._
import cats.data._
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import coop.rchain.rholang.interpreter.CostAccounting.CostStateRef
import coop.rchain.rholang.interpreter.accounting.Cost
import coop.rchain.rholang.interpreter.errors.OutOfPhlogistonsError

object CostAccounting {
  final case class CostState(
      total: Cost = Cost(0),
      trace: Chain[Cost] = Chain.empty
  ) {
    def charge(amount: Cost): (CostState, Boolean) =
      if (total.value < 0) {
        this -> true
      } else {
        val newAmount   = total - amount
        val newTrace    = trace :+ amount
        val newSt       = CostState(newAmount, newTrace)
        val isOutOfPhlo = newAmount.value < 0
        newSt -> isOutOfPhlo
      }

    def reset(amount: Cost): CostState = CostState(amount, Chain())
  }

  type CostStateRef[F[_]] = Ref[F, CostState]

  def CostStateRef[F[_]](implicit instance: CostStateRef[F]): CostStateRef[F] = instance

  def initialCost[F[_]: Sync](amount: Cost): F[Ref[F, CostState]] =
    Ref.of[F, CostState](CostState(amount))

  def emptyCost[F[_]: Sync]: F[Ref[F, CostState]] =
    Ref.of[F, CostState](CostState(Cost(0, "init")))

  implicit class CostStateOps[F[_]](private val cost: CostStateRef[F]) extends AnyVal {
    def set(amount: Cost): F[Unit] = cost.update(_.reset(amount))

    def current(implicit f: Functor[F]): F[Cost] = cost.get.map(_.total)

    // Used in tests
    def <+(amount: Cost): F[Unit] = cost.update(s => s.copy(s.total + amount))
  }
}

package object accounting extends Costs {

  def charge[F[_]: Sync: CostStateRef](amount: Cost): F[Unit] =
    for {
      isError <- CostStateRef[F].modify(_.charge(amount))
      _       <- OutOfPhlogistonsError.raiseError.whenA(isError)
    } yield ()
}

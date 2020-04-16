package coop.rchain.casper.state

import cats.tagless.finalAlg
import coop.rchain.rspace.Blake2b256Hash
import coop.rchain.state.StateManager

@finalAlg
trait BlockStateManager[F[_]] extends StateManager[F]

final case class BlockStateStatus()

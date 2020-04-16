package coop.rchain.casper.state

import cats.tagless.finalAlg
import coop.rchain.state.StateManager

@finalAlg
trait RNodeStateManager[F[_]] extends StateManager[F]

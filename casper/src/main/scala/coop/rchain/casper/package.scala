package coop.rchain

import coop.rchain.casper.state.BlockStoreSyntax
import coop.rchain.casper.util.comm.CommUtilSyntax
import coop.rchain.metrics.Metrics
import coop.rchain.models.BlockHash.BlockHash

package object casper {
  type TopoSort             = Vector[Vector[BlockHash]]
  type BlockProcessing[A]   = Either[BlockError, A]
  type ValidBlockProcessing = BlockProcessing[ValidBlock]

  val CasperMetricsSource: Metrics.Source = Metrics.Source(Metrics.BaseSource, "casper")

  // Casper syntax
  object syntax extends CommUtilSyntax with BlockStoreSyntax
}

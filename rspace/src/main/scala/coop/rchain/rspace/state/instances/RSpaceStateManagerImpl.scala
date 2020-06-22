package coop.rchain.rspace.state.instances

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import coop.rchain.catscontrib.Catscontrib._
import coop.rchain.catscontrib.ski._
import coop.rchain.rspace.state.instances.RSpaceExporterImpl.NoRootException
import coop.rchain.rspace.state.{RSpaceExporter, RSpaceImporter, RSpaceStateManager}

object RSpaceStateManagerImpl {
  def apply[F[_]: Sync](
      exporter: RSpaceExporter[F],
      importer: RSpaceImporter[F]
  ): RSpaceStateManager[F] =
    RSpaceStateManagerImpl[F](exporter, importer)

  private final case class RSpaceStateManagerImpl[F[_]: Sync](
      exporter: RSpaceExporter[F],
      importer: RSpaceImporter[F]
  ) extends RSpaceStateManager[F] {

    val status: F[Ref[F, Boolean]] = Ref.of(false)

    override def isEmpty: F[Boolean] = status.flatMap(_.get) &&^ hasRoot

    def hasRoot: F[Boolean] =
      exporter.getRoot.map(kp(true)).handleError {
        case NoRootException => false
      }
  }
}

// TODO: move it to test project
final case class RSpaceStateManagerDummyImpl[F[_]: Sync]() extends RSpaceStateManager[F] {
  override def exporter: RSpaceExporter[F] = ???

  override def importer: RSpaceImporter[F] = ???

  override def isEmpty: F[Boolean] = ???
}

package coop.rchain.rspace.bench

import coop.rchain.rholang.interpreter.{CostAccounting, ParBuilderUtil, Runtime}
import java.nio.file.{Files, Path}

import coop.rchain.catscontrib.TaskContrib._
import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.metrics
import coop.rchain.metrics.{Metrics, NoopSpan, Span}
import coop.rchain.models.Par
import coop.rchain.rholang.interpreter.CostAccounting.CostStateRef
import coop.rchain.rholang.interpreter.accounting._
import coop.rchain.shared.Log
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.concurrent.Await
import scala.concurrent.duration._

abstract class RhoBenchBaseState {

  def setupRho: Option[String] = None
  def testedRho: String

  def execute(bh: Blackhole): Unit = {
    val r = (for {
      result <- runTask
      _      <- runtime.space.createCheckpoint()
    } yield result).unsafeRunSync
    bh.consume(r)
  }

  implicit val scheduler: Scheduler = Scheduler.fixedPool(name = "rho-1", poolSize = 100)
  lazy val dbDir: Path              = Files.createTempDirectory(BenchStorageDirPrefix)
  val mapSize: Long                 = 1024L * 1024L * 1024L * 10L

  var runtime: Runtime[Task]      = null
  var setupTerm: Option[Par]      = None
  var term: Par                   = _
  var randSetup: Blake2b512Random = null
  var randRun: Blake2b512Random   = null

  var runTask: Task[Unit] = null

  implicit val logF: Log[Task]            = Log.log[Task]
  implicit val noopMetrics: Metrics[Task] = new metrics.Metrics.MetricsNOP[Task]
  implicit val noopSpan: Span[Task]       = NoopSpan[Task]()
  implicit val ms: Metrics.Source         = Metrics.BaseSource
  def rand: Blake2b512Random              = Blake2b512Random(128)

  def createRuntime(): Runtime[Task] =
    (for {
      cost <- CostAccounting.emptyCost[Task]
      sar  <- Runtime.setupRSpace[Task](dbDir, mapSize)
      runtime <- {
        implicit val c: CostStateRef[Task] = cost
        Runtime.create[Task]((sar._1, sar._2))
      }
    } yield (runtime)).unsafeRunSync

  @Setup(value = Level.Iteration)
  def doSetup(): Unit = {
    deleteOldStorage(dbDir)
    setupTerm = setupRho.flatMap { p =>
      ParBuilderUtil
        .buildNormalizedTerm[Coeval](p)
        .runAttempt match {
        case Right(par) => Some(par)
        case Left(err)  => throw err
      }
    }

    term = ParBuilderUtil
      .buildNormalizedTerm[Coeval](testedRho)
      .runAttempt match {
      case Right(par) => par
      case Left(err)  => throw err
    }
    runtime = createRuntime()
    runtime.cost.set(Cost.UNSAFE_MAX).runSyncUnsafe(1.second)

    (for {
      emptyCheckpoint <- runtime.space.createCheckpoint()
      //make sure we always start from clean rspace & trie
      _ <- runtime.replaySpace.clear()
      _ <- runtime.replaySpace.reset(emptyCheckpoint.root)
      _ <- runtime.space.clear()
      _ <- runtime.space.reset(emptyCheckpoint.root)
    } yield ()).unsafeRunSync

    randSetup = rand
    randRun = rand
    Await
      .result(
        createTest(setupTerm)(runtime.reducer, randSetup).runToFuture,
        Duration.Inf
      )
    runTask = createTest(Some(term))(runtime.reducer, randRun)
  }

  @TearDown
  def tearDown(): Unit =
    runtime.close().unsafeRunSync
}

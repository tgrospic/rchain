package coop.rchain.rholang.interpreter

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import com.typesafe.scalalogging.Logger
import coop.rchain.crypto.PublicKey
import coop.rchain.crypto.hash.{Blake2b256, Keccak256, Sha256}
import coop.rchain.crypto.signatures.{Ed25519, Secp256k1}
import coop.rchain.models.Expr.ExprInstance.{EListBody, ETupleBody, GBool, GInt, GString}
import coop.rchain.models._
import coop.rchain.models.rholang.implicits._
import coop.rchain.rholang.interpreter.Runtime.{BlockDataStorage, InvalidBlocks, RhoISpace}
import coop.rchain.rholang.interpreter.util.RevAddress
import coop.rchain.rspace.{ContResult, Result, Serialize}
import scalaj.http._
import scala.sys.process._
import scala.util.Try

//TODO: Make each of the system processes into a case class,
//      so that implementation is not repetitive.
//TODO: Make polymorphic over match type.
trait SystemProcesses[F[_]] {
  import SystemProcesses.Contract

  def stdOut: Contract[F]
  def stdOutAck: Contract[F]
  def stdErr: Contract[F]
  def stdErrAck: Contract[F]
  def secp256k1Verify: Contract[F]
  def ed25519Verify: Contract[F]
  def sha256Hash: Contract[F]
  def keccak256Hash: Contract[F]
  def blake2b256Hash: Contract[F]
  def getDeployParams(runtimeParametersRef: Ref[F, DeployParameters]): Contract[F]
  def getBlockData(timestamp: BlockDataStorage[F]): Contract[F]
  def invalidBlocks(invalidBlocks: InvalidBlocks[F]): Contract[F]
  def validateRevAddress: Contract[F]
  def http: Contract[F]
  def serialize: Contract[F]
  def sysProcess: Contract[F]
}

object SystemProcesses {
  type Contract[F[_]] = (Seq[ListParWithRandom], Int) => F[Unit]

  def apply[F[_]](
      dispatcher: Dispatch[F, ListParWithRandom, TaggedContinuation],
      space: RhoISpace[F]
  )(implicit F: Concurrent[F]): SystemProcesses[F] =
    new SystemProcesses[F] {

      type ContWithMetaData = ContResult[Par, BindPattern, TaggedContinuation]

      type Channels = Seq[Result[ListParWithRandom]]

      private val prettyPrinter = PrettyPrinter()

      private val prettyPrinterCmd = PrettyPrinterCmd()

      private val isContractCall = new ContractCall[F](space, dispatcher)

      private val stdOutLogger = Logger("coop.rchain.rholang.stdout")
      private val stdErrLogger = Logger("coop.rchain.rholang.stderr")

      private def illegalArgumentException(msg: String): F[Unit] =
        F.raiseError(new IllegalArgumentException(msg))

      def verifySignatureContract(
          name: String,
          algorithm: (Array[Byte], Array[Byte], Array[Byte]) => Boolean
      ): Contract[F] = {
        case isContractCall(
            produce,
            Seq(
              RhoType.ByteArray(data),
              RhoType.ByteArray(signature),
              RhoType.ByteArray(pub),
              ack
            )
            ) =>
          for {
            verified <- F.fromTry(Try(algorithm(data, signature, pub)))
            _        <- produce(Seq(RhoType.Boolean(verified)), ack)
          } yield ()
        case _ =>
          illegalArgumentException(
            s"$name expects data, signature, public key (all as byte arrays), and an acknowledgement channel"
          )
      }

      def hashContract(name: String, algorithm: Array[Byte] => Array[Byte]): Contract[F] = {
        case isContractCall(produce, Seq(RhoType.ByteArray(input), ack)) =>
          for {
            hash <- F.fromTry(Try(algorithm(input)))
            _    <- produce(Seq(RhoType.ByteArray(hash)), ack)
          } yield ()
        case _ =>
          illegalArgumentException(
            s"$name expects a byte array and return channel"
          )
      }

      @SuppressWarnings(Array("org.wartremover.warts.Throw"))
      def getString(par: Par) =
        par.exprs.head.exprInstance match {
          case GString(s) => s
          case x          => prettyPrinter.buildString(x)
        }

      def http: Contract[F] = {
        case isContractCall(produce, Seq(verbPar, urlPar, postDataPar, ack)) =>
          val reply = (success: Boolean, p: Seq[Par]) =>
            produce(Seq(ETupleBody(ETuple(Seq(GBool(success): Par) ++ p))), ack)
          try {
            val verb = getString(verbPar)
            val url  = getString(urlPar)
            val http =
              if (verb == "POST") {
                // POST request
                val data = prettyPrinter.buildString(postDataPar)
                Http(url)
                  .header("content-type", "application/json")
                  .option(HttpOptions.followRedirects(true))
                  .postData(data)
                  .timeout(connTimeoutMs = 1000, readTimeoutMs = 60000)
                  .asString
              } else {
                // GET request
                Http(url)
                  .option(HttpOptions.followRedirects(true))
                  .timeout(connTimeoutMs = 1000, readTimeoutMs = 60000)
                  .asString
              }
            if (http.code == 200)
              for {
                parRes <- ParBuilder[F].buildNormalizedTerm(http.body).attempt
                _ <- parRes.fold(
                      ex => reply(true, Seq(GString(http.body))),
                      x => reply(true, Seq(x))
                    )
              } yield ()
            else reply(false, Seq(GInt(http.code.toLong)))
          } catch {
            case ex: Throwable => reply(false, Seq(GString(ex.getMessage)))
          }
      }

      def serialize: Contract[F] = {
        case isContractCall(produce, Seq(term, ack)) =>
          val serialized = prettyPrinter.buildString(term)
          produce(Seq(RhoType.String(serialized)), ack)
      }

      def sysProcess: Contract[F] = {
        case isContractCall(produce, Seq(cmdPar, par, ack)) =>
          val reply = (success: Boolean, p: Seq[Par]) =>
            produce(Seq(ETupleBody(ETuple(Seq(GBool(success): Par) ++ p))), ack)
          try {
            val cmd     = getString(cmdPar)
            val execCmd = buildSeqString(par.exprs.head)
            Console.println("Run command: " + execCmd)
            val output = execCmd.!!
            cmd match {
              case "run" => reply(true, Seq(GString(output)))
              case "run:rho" => {
                for {
                  parRes <- ParBuilder[F].buildNormalizedTerm(output).attempt
                  _ <- parRes.fold(
                        ex => reply(false, Seq(GString(ex.getMessage), GString(output))),
                        x => reply(true, Seq(x))
                      )
                } yield ()
              }
            }
          } catch {
            case ex: Throwable => reply(false, Seq(GString(ex.getMessage), GString("")))
          }
      }

//      def sysProcessStream: Contract[F] = {
//        case isContractCall(produce, Seq(cmdPar, par, ack)) =>
//          val reply = (success: Boolean, p: Seq[Par]) =>
//            produce(Seq(ETupleBody(ETuple(Seq(GBool(success): Par) ++ p))), ack)
//          try {
//            val io = new ProcessIO(writer, out => {
//              scala.io.Source.fromInputStream(out).getLines.foreach(println)
//            }, err => { scala.io.Source.fromInputStream(err).getLines.foreach(println) })
//
//            def writer(output: java.io.OutputStream) =
//              output.write("Sent from RNode writer stream".getBytes)
//
//            val cmd     = getString(cmdPar)
//            val process = Process(cmd)
//            process run io
//
//            val execCmd = buildSeqString(par.exprs.head)
//            println("Run command: " + execCmd)
//            val output = execCmd.!!
//            cmd match {
//              case "run" => reply(true, Seq(GString(output)))
//              case "run:rho" => {
//                for {
//                  parRes <- ParBuilder[F].buildNormalizedTerm(output).attempt
//                  _ <- parRes.fold(
//                        ex => reply(false, Seq(GString(ex.getMessage), GString(output))),
//                        x => reply(true, Seq(x))
//                      )
//                } yield ()
//              }
//            }
//          } catch {
//            case ex: Throwable => reply(false, Seq(GString(ex.getMessage), GString("")))
//          }
//      }

      def buildSeqString(e: Expr): Seq[String] =
        e.exprInstance match {
          case EListBody(EList(s, _, _, _)) => s.map(prettyPrinterCmd.buildString)
          case GString(s)                   => Seq(s)
          case _                            => Seq(prettyPrinter.buildString(e))
        }

      private def printStdOut(s: String): F[Unit] =
        for {
          _ <- F.delay(Console.println(s))
          _ <- F.delay(stdOutLogger.debug(s))
        } yield ()

      private def printStdErr(s: String): F[Unit] =
        for {
          _ <- F.delay(Console.err.println(s))
          _ <- F.delay(stdErrLogger.debug(s))
        } yield ()

      def stdOut: Contract[F] = {
        case isContractCall(_, Seq(arg)) =>
          printStdOut(prettyPrinter.buildString(arg))
      }

      def stdOutAck: Contract[F] = {
        case isContractCall(produce, Seq(arg, ack)) =>
          for {
            _ <- printStdOut(prettyPrinter.buildString(arg))
            _ <- produce(Seq(Par.defaultInstance), ack)
          } yield ()
      }

      def stdErr: Contract[F] = {
        case isContractCall(_, Seq(arg)) =>
          printStdErr(prettyPrinter.buildString(arg))
      }

      def stdErrAck: Contract[F] = {
        case isContractCall(produce, Seq(arg, ack)) =>
          for {
            _ <- printStdErr(prettyPrinter.buildString(arg))
            _ <- produce(Seq(Par.defaultInstance), ack)
          } yield ()
      }

      def validateRevAddress: Contract[F] = {
        case isContractCall(
            produce,
            Seq(RhoType.String("validate"), RhoType.String(address), ack)
            ) =>
          val errorMessage =
            RevAddress
              .parse(address)
              .swap
              .toOption
              .map(RhoType.String(_))
              .getOrElse(Par())

          produce(Seq(errorMessage), ack)

        case isContractCall(
            produce,
            Seq(RhoType.String("fromPublicKey"), RhoType.ByteArray(publicKey), ack)
            ) =>
          val response =
            RevAddress
              .fromPublicKey(PublicKey(publicKey))
              .map(ra => RhoType.String(ra.toBase58))
              .getOrElse(Par())

          produce(Seq(response), ack)
      }

      def secp256k1Verify: Contract[F] =
        verifySignatureContract("secp256k1Verify", Secp256k1.verify)

      def ed25519Verify: Contract[F] =
        verifySignatureContract("ed25519Verify", Ed25519.verify)

      def sha256Hash: Contract[F] =
        hashContract("sha256Hash", Sha256.hash)

      def keccak256Hash: Contract[F] =
        hashContract("keccak256Hash", Keccak256.hash)

      def blake2b256Hash: Contract[F] =
        hashContract("blake2b256Hash", Blake2b256.hash)

      // TODO: rename this system process to "deployParameters"?
      def getDeployParams(runtimeParametersRef: Ref[F, DeployParameters]): Contract[F] = {
        case isContractCall(produce, Seq(ack)) =>
          for {
            params                                                  <- runtimeParametersRef.get
            DeployParameters(codeHash, phloRate, userId, timestamp) = params
            _ <- produce(
                  Seq(codeHash, phloRate, userId, timestamp),
                  ack
                )
          } yield ()
        case _ =>
          illegalArgumentException("deployParameters expects only a return channel")
      }

      def getBlockData(
          blockData: Runtime.BlockDataStorage[F]
      ): Contract[F] = {
        case isContractCall(produce, Seq(ack)) =>
          for {
            maybeData <- blockData.get
            (number, time) = maybeData
              .map(data => (RhoType.Number(data.blockNumber), RhoType.Number(data.timeStamp)))
              .getOrElse((Par(), Par()))
            _ <- produce(Seq(number, time), ack)
          } yield ()
        case _ =>
          illegalArgumentException("blockTime expects only a return channel")
      }

      def invalidBlocks(invalidBlocks: Runtime.InvalidBlocks[F]): Contract[F] = {
        case isContractCall(produce, Seq(ack)) =>
          for {
            invalidBlocks <- invalidBlocks.invalidBlocks.get
            _             <- produce(Seq(invalidBlocks), ack)
          } yield ()
        case _ =>
          illegalArgumentException("invalidBlocks expects only a return channel")
      }
    }
}

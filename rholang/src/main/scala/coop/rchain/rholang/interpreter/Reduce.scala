package coop.rchain.rholang.interpreter

import cats.effect.Sync
import cats.implicits._
import cats.mtl.FunctorTell
import cats.{Eval => _}
import com.google.protobuf.ByteString
import coop.rchain.crypto.codec.Base16
import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.models.Expr.ExprInstance._
import coop.rchain.models.TaggedContinuation.TaggedCont.ParBody
import coop.rchain.models.Var.VarInstance
import coop.rchain.models.Var.VarInstance.{BoundVar, FreeVar, Wildcard}
import coop.rchain.models.rholang.implicits._
import coop.rchain.models.serialization.implicits._
import coop.rchain.models.{Match, MatchCase, _}
import coop.rchain.rholang.interpreter.Runtime.{RhoDispatch, RhoPureSpace}
import coop.rchain.rholang.interpreter.Substitute.{charge => _, _}
import coop.rchain.rholang.interpreter.accounting._
import coop.rchain.rholang.interpreter.errors._
import coop.rchain.rholang.interpreter.matcher.SpatialMatcher.spatialMatchAndCharge
import coop.rchain.rspace.Serialize
import coop.rchain.rspace.util._
import monix.eval.Coeval

import scala.collection.immutable.BitSet
import scala.util.Try

/** Reduce is the interface for evaluating Rholang expressions.
  *
  * @tparam M The kind of Monad used for evaluation.
  */
trait Reduce[M[_]] {

  def eval(par: Par)(
      implicit env: Env[Par],
      rand: Blake2b512Random,
      sequenceNumber: Int
  ): M[Unit]

  def inj(
      par: Par
  )(implicit rand: Blake2b512Random): M[Unit]

  /**
    * Evaluate any top level expressions in @param Par .
    */
  def evalExpr(par: Par)(implicit env: Env[Par]): M[Par]

  def evalExprToPar(
      expr: Expr
  )(implicit env: Env[Par]): M[Par]
}

class DebruijnInterpreter[M[_], F[_]](
    pureRSpace: RhoPureSpace[M],
    dispatcher: => RhoDispatch[M],
    urnMap: Map[String, Par]
)(
    implicit parallel: cats.Parallel[M, F],
    s: Sync[M],
    fTell: FunctorTell[M, Throwable],
    cost: _cost[M]
) extends Reduce[M] {

  /**
    * Materialize a send in the store, optionally returning the matched continuation.
    *
    * @param chan  The channel on which data is being sent.
    * @param data  The par objects holding the processes being sent.
    * @param persistent  True if the write should remain in the tuplespace indefinitely.
    * @return  An optional continuation resulting from a match in the tuplespace.
    */
  private def produce(
      chan: Par,
      data: ListParWithRandom,
      persistent: Boolean,
      sequenceNumber: Int
  ): M[Unit] = {
    def go(res: Option[(TaggedContinuation, Seq[ListParWithRandom], Int)]): M[Unit] =
      res match {
        case Some((continuation, dataList, updatedSequenceNumber)) =>
          if (persistent)
            List(
              dispatcher.dispatch(continuation, dataList, updatedSequenceNumber),
              produce(chan, data, persistent, sequenceNumber)
            ).parSequence_
          else dispatcher.dispatch(continuation, dataList, updatedSequenceNumber)
        case None => s.unit
      }
    pureRSpace.produce(chan, data, persist = persistent, sequenceNumber) >>= (go(_))
  }

  /**
    * Materialize a send in the store, optionally returning the matched continuation.
    *
    * @param binds  A Seq of pattern, channel pairs. Each pattern is a Seq[Par].
    *               The Seq is for arity matching, and each term in the Seq is a name pattern.
    * @param body  A Par object which will be run in the envirnoment resulting from the match.
    * @return  An optional continuation resulting from a match. The body of the continuation
    *          will be @param body if the continuation is not None.
    */
  private def consume(
      binds: Seq[(BindPattern, Par)],
      body: ParWithRandom,
      persistent: Boolean,
      sequenceNumber: Int
  ): M[Unit] = {
    val (patterns: Seq[BindPattern], sources: Seq[Par]) = binds.unzip
    def go(res: Option[(TaggedContinuation, Seq[ListParWithRandom], Int)]): M[Unit] =
      res match {
        case Some((continuation, dataList, updatedSequenceNumber)) =>
          if (persistent)
            List(
              dispatcher.dispatch(continuation, dataList, updatedSequenceNumber),
              consume(binds, body, persistent, sequenceNumber)
            ).parSequence_
          else dispatcher.dispatch(continuation, dataList, updatedSequenceNumber)
        case None => s.unit
      }
    pureRSpace.consume(
      sources.toList,
      patterns.toList,
      TaggedContinuation(ParBody(body)),
      persist = persistent,
      sequenceNumber
    ) >>= (go(_))
  }

  private trait EvalJob {
    def run(mkRand: Int => Blake2b512Random)(
        env: Env[Par],
        sequenceNumber: Int
    ): M[Unit]
    def size: Int
  }

  private object EvalJob {

    private def mkJob[A](
        input: Seq[A],
        handler: A => (Env[Par], Blake2b512Random, Int) => M[Unit]
    ): EvalJob =
      new EvalJob {
        override def run(mkRand: Int => Blake2b512Random)(
            env: Env[Par],
            sequenceNumber: Int
        ): M[Unit] =
          input.zipWithIndex.toList.parTraverse_ {
            case (term, idx) =>
              handler(term)(env, mkRand(idx), sequenceNumber)
          }

        override def size: Int = input.size
      }

    def apply(exprs: Seq[Expr]): EvalJob = {
      def handler(
          expr: Expr
      )(
          env: Env[Par],
          rand: Blake2b512Random,
          sequenceNumber: Int
      ): M[Unit] =
        expr.exprInstance match {
          case EVarBody(EVar(v)) =>
            (for {
              varref <- eval(v)(env)
              _      <- eval(varref)(env, rand, sequenceNumber)
            } yield ()).handleErrorWith(fTell.tell)
          case e: EMethodBody =>
            (for {
              p <- evalExprToPar(Expr(e))(env)
              _ <- eval(p)(env, rand, sequenceNumber)
            } yield ()).handleErrorWith(fTell.tell)
          case _ => ().pure[M]
        }

      mkJob(exprs, handler)
    }

    def apply[A](
        terms: Seq[A],
        handlerImpl: A => (Env[Par], Blake2b512Random, Int) => M[Unit]
    ): EvalJob = {
      def handler(
          term: A
      )(
          env: Env[Par],
          rand: Blake2b512Random,
          sequenceNumber: Int
      ): M[Unit] =
        handlerImpl(term)(env, rand, sequenceNumber)
          .handleErrorWith {
            case e: OutOfPhlogistonsError.type =>
              e.raiseError[M, Unit]
            case e =>
              fTell.tell(e) >> ().pure[M]
          }

      mkJob(terms, handler)
    }

  }

  override def eval(par: Par)(
      implicit env: Env[Par],
      rand: Blake2b512Random,
      sequenceNumber: Int
  ): M[Unit] = {
    def split(totalSize: Int, termSize: Int, rand: Blake2b512Random)(idx: Int): Blake2b512Random =
      if (totalSize == 1)
        rand
      else if (totalSize > 256)
        rand.splitShort((termSize + idx).toShort)
      else
        rand.splitByte((termSize + idx).toByte)

    val filteredExprs = par.exprs.filter { expr =>
      expr.exprInstance match {
        case _: EVarBody    => true
        case _: EMethodBody => true
        case _              => false
      }
    }

    val jobs = List(
      EvalJob[Send](par.sends, evalExplicit),
      EvalJob[Receive](par.receives, evalExplicit),
      EvalJob[New](par.news, evalExplicit),
      EvalJob[Match](par.matches, evalExplicit),
      EvalJob[Bundle](par.bundles, evalExplicit),
      EvalJob(filteredExprs)
    ).filter(_.size > 0)

    val starts = jobs.map(_.size).scanLeft(0)(_ + _).toVector

    jobs.zipWithIndex.parTraverse_ {
      case (job, jobIdx) => {
        def mkRand(termIdx: Int): Blake2b512Random =
          split(starts.last, starts(jobIdx), rand)(termIdx)
        job.run(mkRand)(env, sequenceNumber)
      }
    }
  }

  override def inj(
      par: Par
  )(implicit rand: Blake2b512Random): M[Unit] =
    eval(par)(Env[Par](), rand, 0)

  private def evalExplicit(
      send: Send
  )(
      env: Env[Par],
      rand: Blake2b512Random,
      sequenceNumber: Int
  ): M[Unit] =
    eval(send)(env, rand, sequenceNumber)

  /** Algorithm as follows:
    *
    * 1. Fully evaluate the channel in given environment.
    * 2. Substitute any variable references in the channel so that it can be
    *    correctly used as a key in the tuple space.
    * 3. Evaluate any top level expressions in the data being sent.
    * 4. Call produce
    * 5. If produce returned a continuation, evaluate it.
    *
    * @param send An output process
    * @param env An execution context
    *
    */
  private def eval(send: Send)(
      implicit env: Env[Par],
      rand: Blake2b512Random,
      sequenceNumber: Int
  ): M[Unit] =
    for {
      evalChan <- evalExpr(send.chan)
      subChan  <- substituteAndCharge[Par, M](evalChan, 0, env)
      unbundled <- subChan.singleBundle() match {
                    case Some(value) =>
                      if (!value.writeFlag)
                        ReduceError("Trying to send on non-writeable channel.").raiseError[M, Par]
                      else value.body.pure[M]
                    case None => subChan.pure[M]
                  }
      data      <- send.data.toList.traverse(evalExpr)
      substData <- data.traverse(substituteAndCharge[Par, M](_, 0, env))
      _         <- produce(unbundled, ListParWithRandom(substData, rand), send.persistent, sequenceNumber)
      _         <- charge[M](SEND_EVAL_COST)
    } yield ()

  private def evalExplicit(
      receive: Receive
  )(
      env: Env[Par],
      rand: Blake2b512Random,
      sequenceNumber: Int
  ): M[Unit] =
    eval(receive)(env, rand, sequenceNumber)

  private def eval(receive: Receive)(
      implicit env: Env[Par],
      rand: Blake2b512Random,
      sequenceNumber: Int
  ): M[Unit] =
    for {
      binds <- receive.binds.toList.traverse(
                rb =>
                  for {
                    q <- unbundleReceive(rb)
                    substPatterns <- rb.patterns.toList
                                      .traverse(substituteAndCharge[Par, M](_, 1, env))
                  } yield (BindPattern(substPatterns, rb.remainder, rb.freeCount), q)
              )
      // TODO: Allow for the environment to be stored with the body in the Tuplespace
      substBody <- substituteNoSortAndCharge[Par, M](
                    receive.body,
                    0,
                    env.shift(receive.bindCount)
                  )
      _ <- consume(binds, ParWithRandom(substBody, rand), receive.persistent, sequenceNumber)
      _ <- charge[M](RECEIVE_EVAL_COST)
    } yield ()

  /**
    * Variable "evaluation" is an environment lookup, but
    * lookup of an unbound variable should be an error.
    *
    * @param valproc The variable to be evaluated
    * @param env  provides the environment (possibly) containing a binding for the given variable.
    * @return If the variable has a binding (par), lift the
    *                  binding into the monadic context, else signal
    *                  an exception.
    *
    */
  private def eval(
      valproc: Var
  )(implicit env: Env[Par]): M[Par] =
    charge[M](VAR_EVAL_COST) >> {
      valproc.varInstance match {
        case BoundVar(level) =>
          env.get(level) match {
            case Some(par) =>
              par.pure[M]
            case None =>
              ReduceError("Unbound variable: " + level + " in " + env.envMap).raiseError[M, Par]
          }
        case Wildcard(_) =>
          ReduceError("Unbound variable: attempting to evaluate a pattern").raiseError[M, Par]
        case FreeVar(_) =>
          ReduceError("Unbound variable: attempting to evaluate a pattern").raiseError[M, Par]
        case VarInstance.Empty =>
          ReduceError("Impossible var instance EMPTY").raiseError[M, Par]
      }
    }

  private def evalExplicit(
      mat: Match
  )(
      env: Env[Par],
      rand: Blake2b512Random,
      sequenceNumber: Int
  ): M[Unit] =
    eval(mat)(env, rand, sequenceNumber)

  private def eval(mat: Match)(
      implicit env: Env[Par],
      rand: Blake2b512Random,
      sequenceNumber: Int
  ): M[Unit] = {

    def addToEnv(env: Env[Par], freeMap: Map[Int, Par], freeCount: Int): Env[Par] =
      Range(0, freeCount).foldLeft(env)(
        (acc, e) =>
          acc.put(
            freeMap.get(e) match {
              case Some(p) => p
              case None    => Par()
            }
          )
      )

    def firstMatch(target: Par, cases: Seq[MatchCase])(implicit env: Env[Par]): M[Unit] = {
      def firstMatchM(
          state: (Par, Seq[MatchCase])
      ): M[Either[(Par, Seq[MatchCase]), Unit]] = {
        val (target, cases) = state
        cases match {
          case Nil => ().asRight[(Par, Seq[MatchCase])].pure[M]
          case singleCase +: caseRem =>
            for {
              pattern     <- substituteAndCharge[Par, M](singleCase.pattern, 1, env)
              matchResult <- spatialMatchAndCharge[M](target, pattern)
              res <- matchResult match {
                      case None =>
                        (target, caseRem).asLeft[Unit].pure[M]
                      case Some((freeMap, _)) =>
                        eval(singleCase.source)(
                          addToEnv(env, freeMap, singleCase.freeCount),
                          implicitly,
                          sequenceNumber
                        ).map(_.asRight[(Par, Seq[MatchCase])])
                    }
            } yield res
        }
      }
      (target, cases).tailRecM(firstMatchM)
    }

    for {
      evaledTarget <- evalExpr(mat.target)
      // TODO(kyle): Make the matcher accept an environment, instead of substituting it.
      substTarget <- substituteAndCharge[Par, M](evaledTarget, 0, env)
      _           <- firstMatch(substTarget, mat.cases)
      _           <- charge[M](MATCH_EVAL_COST)
    } yield ()
  }

  /**
    * Adds neu.bindCount new GPrivate from UUID's to the environment and then
    * proceeds to evaluate the body.
    *
    * @param neu
    * @return
    */
  private def evalExplicit(
      neu: New
  )(env: Env[Par], rand: Blake2b512Random, sequenceNumber: Int): M[Unit] =
    eval(neu)(env, rand, sequenceNumber)

  // TODO: Eliminate variable shadowing
  private def eval(
      neu: New
  )(implicit env: Env[Par], rand: Blake2b512Random, sequenceNumber: Int): M[Unit] = {

    def alloc(count: Int, urns: Seq[String]): M[Env[Par]] = {
      val deployIdUrn   = "rho:rchain:deployId"
      val deployerIdUrn = "rho:rchain:deployerId"

      val simpleNews = (0 until (count - urns.size)).toList.foldLeft(env) { (_env, _) =>
        val addr: Par = GPrivate(ByteString.copyFrom(rand.next()))
        _env.put(addr)
      }

      def normalizerBugFound(name: String, urn: String) =
        BugFoundError(
          s"No $name set despite `$urn` being used in a term. This is a bug in the normalizer or on the path from it."
        )

      def addUrn(newEnv: Env[Par], urn: String): Either[InterpreterError, Env[Par]] =
        if (urn == deployIdUrn)
          neu.deployId
            .map { case DeployId(sig) => newEnv.put(GDeployId(sig)).asRight[InterpreterError] }
            .getOrElse(normalizerBugFound("DeployId", deployIdUrn).asLeft[Env[Par]])
        else if (urn == deployerIdUrn)
          neu.deployerId
            .map { case DeployerId(pk) => newEnv.put(GDeployerId(pk)).asRight[InterpreterError] }
            .getOrElse(normalizerBugFound("DeployerId", deployerIdUrn).asLeft[Env[Par]])
        else
          urnMap.get(urn) match {
            case Some(p) => newEnv.put(p).asRight[InterpreterError]
            case None    => ReduceError(s"Unknown urn for new: $urn").asLeft[Env[Par]]
          }

      urns.toList.foldM(simpleNews)(addUrn) match {
        case Right(tmpEnv) => tmpEnv.pure[M]
        case Left(e)       => e.raiseError[M, Env[Par]]
      }
    }

    charge[M](newBindingsCost(neu.bindCount)) >>
      alloc(neu.bindCount, neu.uri).flatMap(eval(neu.p)(_, rand, sequenceNumber))
  }

  private[this] def unbundleReceive(rb: ReceiveBind)(implicit env: Env[Par]): M[Par] =
    for {
      evalSrc <- evalExpr(rb.source)
      subst   <- substituteAndCharge[Par, M](evalSrc, 0, env)
      // Check if we try to read from bundled channel
      unbndl <- subst.singleBundle() match {
                 case Some(value) =>
                   if (!value.readFlag)
                     ReduceError("Trying to read from non-readable channel.").raiseError[M, Par]
                   else value.body.pure[M]
                 case None => subst.pure[M]
               }
    } yield unbndl

  private def evalExplicit(
      bundle: Bundle
  )(env: Env[Par], rand: Blake2b512Random, sequenceNumber: Int): M[Unit] =
    eval(bundle)(env, rand, sequenceNumber)

  private def eval(
      bundle: Bundle
  )(implicit env: Env[Par], rand: Blake2b512Random, sequenceNumber: Int): M[Unit] =
    eval(bundle.body)

  def evalExprToPar(expr: Expr)(implicit env: Env[Par]): M[Par] =
    expr.exprInstance match {
      case EVarBody(EVar(v)) =>
        for {
          p       <- eval(v)
          evaledP <- evalExpr(p)
        } yield evaledP
      case EMethodBody(EMethod(method, target, arguments, _, _)) => {
        for {
          _            <- charge[M](METHOD_CALL_COST)
          evaledTarget <- evalExpr(target)
          evaledArgs   <- arguments.toList.traverse(evalExpr)
          resultPar <- methodTable.get(method) match {
                        case None =>
                          ReduceError("Unimplemented method: " + method).raiseError[M, Par]
                        case Some(f) => f(evaledTarget, evaledArgs)
                      }
        } yield resultPar
      }
      case _ => evalExprToExpr(expr).map(fromExpr(_)(identity))
    }

  private def evalExprToExpr(expr: Expr)(implicit env: Env[Par]): M[Expr] = Sync[M].defer {
    def relop(
        p1: Par,
        p2: Par,
        relopb: (Boolean, Boolean) => Boolean,
        relopi: (Long, Long) => Boolean,
        relops: (String, String) => Boolean
    ): M[Expr] =
      for {
        v1 <- evalSingleExpr(p1)
        v2 <- evalSingleExpr(p2)
        result <- (v1.exprInstance, v2.exprInstance) match {
                   case (GBool(b1), GBool(b2))     => GBool(relopb(b1, b2)).pure[M]
                   case (GInt(i1), GInt(i2))       => GBool(relopi(i1, i2)).pure[M]
                   case (GString(s1), GString(s2)) => GBool(relops(s1, s2)).pure[M]
                   case _ =>
                     ReduceError("Unexpected compare: " + v1 + " vs. " + v2).raiseError[M, GBool]
                 }
      } yield result

    expr.exprInstance match {
      case x: GBool          => (x: Expr).pure[M]
      case x: GInt           => (x: Expr).pure[M]
      case x: GString        => (x: Expr).pure[M]
      case x: GUri           => (x: Expr).pure[M]
      case x: GByteArray     => (x: Expr).pure[M]
      case ENotBody(ENot(p)) => evalToBool(p).map(b => GBool(!b))
      case ENegBody(ENeg(p)) => evalToLong(p).map(v => GInt(-v))
      case EMultBody(EMult(p1, p2)) =>
        for {
          v1 <- evalToLong(p1)
          v2 <- evalToLong(p2)
          _  <- charge[M](MULTIPLICATION_COST)
        } yield GInt(v1 * v2)

      case EDivBody(EDiv(p1, p2)) =>
        for {
          v1 <- evalToLong(p1)
          v2 <- evalToLong(p2)
          _  <- charge[M](DIVISION_COST)
        } yield GInt(v1 / v2)

      case EModBody(EMod(p1, p2)) =>
        for {
          v1 <- evalToLong(p1)
          v2 <- evalToLong(p2)
          _  <- charge[M](MODULO_COST)
        } yield GInt(v1 % v2)

      case EPlusBody(EPlus(p1, p2)) =>
        for {
          v1 <- evalSingleExpr(p1)
          v2 <- evalSingleExpr(p2)
          result <- (v1.exprInstance, v2.exprInstance) match {
                     case (GInt(lhs), GInt(rhs)) =>
                       charge[M](SUM_COST) >> Expr(GInt(lhs + rhs)).pure[M]
                     case (lhs: ESetBody, rhs) =>
                       for {
                         _         <- charge[M](OP_CALL_COST)
                         resultPar <- add(lhs, List[Par](rhs))
                         resultExp <- evalSingleExpr(resultPar)
                       } yield resultExp
                     case (_: GInt, other) =>
                       OperatorExpectedError("+", "Int", other.typ).raiseError[M, Expr]
                     case (other, _) => OperatorNotDefined("+", other.typ).raiseError[M, Expr]
                   }
        } yield result

      case EMinusBody(EMinus(p1, p2)) =>
        for {
          v1 <- evalSingleExpr(p1)
          v2 <- evalSingleExpr(p2)
          result <- (v1.exprInstance, v2.exprInstance) match {
                     case (GInt(lhs), GInt(rhs)) =>
                       charge[M](SUBTRACTION_COST) >> Expr(GInt(lhs - rhs)).pure[M]
                     case (lhs: EMapBody, rhs) =>
                       for {
                         _         <- charge[M](OP_CALL_COST)
                         resultPar <- delete(lhs, List[Par](rhs))
                         resultExp <- evalSingleExpr(resultPar)
                       } yield resultExp
                     case (lhs: ESetBody, rhs) =>
                       for {
                         _         <- charge[M](OP_CALL_COST)
                         resultPar <- delete(lhs, List[Par](rhs))
                         resultExp <- evalSingleExpr(resultPar)
                       } yield resultExp
                     case (_: GInt, other) =>
                       OperatorExpectedError("-", "Int", other.typ).raiseError[M, Expr]
                     case (other, _) =>
                       OperatorNotDefined("-", other.typ).raiseError[M, Expr]
                   }
        } yield result

      case ELtBody(ELt(p1, p2)) =>
        charge[M](COMPARISON_COST) >> relop(p1, p2, (_ < _), (_ < _), (_ < _))

      case ELteBody(ELte(p1, p2)) =>
        charge[M](COMPARISON_COST) >> relop(p1, p2, (_ <= _), (_ <= _), (_ <= _))

      case EGtBody(EGt(p1, p2)) =>
        charge[M](COMPARISON_COST) >> relop(p1, p2, (_ > _), (_ > _), (_ > _))

      case EGteBody(EGte(p1, p2)) =>
        charge[M](COMPARISON_COST) >> relop(p1, p2, (_ >= _), (_ >= _), (_ >= _))

      case EEqBody(EEq(p1, p2)) =>
        for {
          v1 <- evalExpr(p1)
          v2 <- evalExpr(p2)
          // TODO: build an equality operator that takes in an environment.
          sv1 <- substituteAndCharge[Par, M](v1, 0, env)
          sv2 <- substituteAndCharge[Par, M](v2, 0, env)
          _   <- charge[M](equalityCheckCost(sv1, sv2))
        } yield GBool(sv1 == sv2)

      case ENeqBody(ENeq(p1, p2)) =>
        for {
          v1  <- evalExpr(p1)
          v2  <- evalExpr(p2)
          sv1 <- substituteAndCharge[Par, M](v1, 0, env)
          sv2 <- substituteAndCharge[Par, M](v2, 0, env)
          _   <- charge[M](equalityCheckCost(sv1, sv2))
        } yield GBool(sv1 != sv2)

      case EAndBody(EAnd(p1, p2)) =>
        for {
          b1 <- evalToBool(p1)
          b2 <- evalToBool(p2)
          _  <- charge[M](BOOLEAN_AND_COST)
        } yield GBool(b1 && b2)

      case EOrBody(EOr(p1, p2)) =>
        for {
          b1 <- evalToBool(p1)
          b2 <- evalToBool(p2)
          _  <- charge[M](BOOLEAN_OR_COST)
        } yield GBool(b1 || b2)

      case EMatchesBody(EMatches(target, pattern)) =>
        for {
          evaledTarget <- evalExpr(target)
          substTarget  <- substituteAndCharge[Par, M](evaledTarget, 0, env)
          substPattern <- substituteAndCharge[Par, M](pattern, 1, env)
          matchResult  <- spatialMatchAndCharge[M](substTarget, substPattern)
        } yield GBool(matchResult.isDefined)

      case EPercentPercentBody(EPercentPercent(p1, p2)) =>
        def evalToStringPair(keyExpr: Expr, valueExpr: Expr): M[(String, String)] =
          (keyExpr.exprInstance, valueExpr.exprInstance) match {
            case (GString(keyString), GString(valueString)) =>
              (keyString -> valueString).pure[M]
            case (GString(keyString), GInt(valueInt)) =>
              (keyString -> valueInt.toString).pure[M]
            case (GString(keyString), GBool(valueBool)) =>
              (keyString -> valueBool.toString).pure[M]
            case (GString(keyString), GUri(uri)) =>
              (keyString -> uri).pure[M]
            // TODO: Add cases for other ground terms as well? Maybe it would be better
            // to implement cats.Show for all ground terms.
            case (_: GString, value) =>
              ReduceError(s"Error: interpolation doesn't support ${value.typ}")
                .raiseError[M, (String, String)]
            case _ =>
              ReduceError("Error: interpolation Map should only contain String keys")
                .raiseError[M, (String, String)]
          }
        @SuppressWarnings(
          Array("org.wartremover.warts.Var", "org.wartremover.warts.NonUnitStatements")
        )
        // TODO consider replacing while loop with tailrec recursion
        def interpolate(string: String, keyValuePairs: List[(String, String)]): String = {
          val result  = StringBuilder.newBuilder
          var current = string
          while (current.nonEmpty) {
            keyValuePairs.find {
              case (k, _) => current.startsWith("${" + k + "}")
            } match {
              case Some((k, v)) =>
                result ++= v
                current = current.drop(k.length + 3)
              case None =>
                result += current.head
                current = current.tail
            }
          }
          result.toString
        }
        for {
          _  <- charge[M](OP_CALL_COST)
          v1 <- evalSingleExpr(p1)
          v2 <- evalSingleExpr(p2)
          result <- (v1.exprInstance, v2.exprInstance) match {
                     case (GString(lhs), EMapBody(ParMap(rhs, _, _, _))) =>
                       if (lhs.nonEmpty || rhs.nonEmpty) {
                         for {
                           result <- rhs.toList
                                      .traverse {
                                        case (k, v) =>
                                          for {
                                            keyExpr   <- evalSingleExpr(k)
                                            valueExpr <- evalSingleExpr(v)
                                            result    <- evalToStringPair(keyExpr, valueExpr)
                                          } yield result
                                      }
                                      .map(
                                        keyValuePairs => GString(interpolate(lhs, keyValuePairs))
                                      )
                           _ <- charge[M](interpolateCost(lhs.length, rhs.size))
                         } yield result
                       } else GString(lhs).pure[M]
                     case (_: GString, other) =>
                       OperatorExpectedError("%%", "Map", other.typ).raiseError[M, GString]
                     case (other, _) => OperatorNotDefined("%%", other.typ).raiseError[M, GString]
                   }
        } yield result

      case EPlusPlusBody(EPlusPlus(p1, p2)) =>
        for {
          _  <- charge[M](OP_CALL_COST)
          v1 <- evalSingleExpr(p1)
          v2 <- evalSingleExpr(p2)
          result <- (v1.exprInstance, v2.exprInstance) match {
                     case (GString(lhs), GString(rhs)) =>
                       charge[M](stringAppendCost(lhs.length, rhs.length)) >>
                         Expr(GString(lhs + rhs)).pure[M]
                     case (GByteArray(lhs), GByteArray(rhs)) =>
                       charge[M](byteArrayAppendCost(lhs)) >>
                         Expr(GByteArray(lhs.concat(rhs))).pure[M]
                     case (EListBody(lhs), EListBody(rhs)) =>
                       charge[M](listAppendCost(rhs.ps.toVector)) >>
                         Expr(
                           EListBody(
                             EList(
                               lhs.ps ++ rhs.ps,
                               lhs.locallyFree union rhs.locallyFree,
                               lhs.connectiveUsed || rhs.connectiveUsed
                             )
                           )
                         ).pure[M]
                     case (lhs: EMapBody, rhs: EMapBody) =>
                       for {
                         resultPar <- union(lhs, List[Par](rhs))
                         resultExp <- evalSingleExpr(resultPar)
                       } yield resultExp
                     case (lhs: ESetBody, rhs: ESetBody) =>
                       for {
                         resultPar <- union(lhs, List[Par](rhs))
                         resultExp <- evalSingleExpr(resultPar)
                       } yield resultExp
                     case (_: GString, other) =>
                       OperatorExpectedError("++", "String", other.typ).raiseError[M, Expr]
                     case (_: EListBody, other) =>
                       OperatorExpectedError("++", "List", other.typ).raiseError[M, Expr]
                     case (_: EMapBody, other) =>
                       OperatorExpectedError("++", "Map", other.typ).raiseError[M, Expr]
                     case (_: ESetBody, other) =>
                       OperatorExpectedError("++", "Set", other.typ).raiseError[M, Expr]
                     case (other, _) => OperatorNotDefined("++", other.typ).raiseError[M, Expr]
                   }
        } yield result

      case EMinusMinusBody(EMinusMinus(p1, p2)) =>
        for {
          _  <- charge[M](OP_CALL_COST)
          v1 <- evalSingleExpr(p1)
          v2 <- evalSingleExpr(p2)
          result <- (v1.exprInstance, v2.exprInstance) match {
                     case (lhs: ESetBody, rhs: ESetBody) =>
                       for {
                         resultPar <- diff(lhs, List[Par](rhs))
                         resultExp <- evalSingleExpr(resultPar)
                       } yield resultExp
                     case (_: ESetBody, other) =>
                       OperatorExpectedError("--", "Set", other.typ).raiseError[M, Expr]
                     case (other, _) => OperatorNotDefined("--", other.typ).raiseError[M, Expr]
                   }
        } yield result

      case EVarBody(EVar(v)) =>
        for {
          p       <- eval(v)
          exprVal <- evalSingleExpr(p)
        } yield exprVal

      case EListBody(el) =>
        for {
          evaledPs  <- el.ps.toList.traverse(evalExpr)
          updatedPs = evaledPs.map(updateLocallyFree)
        } yield updateLocallyFree(EList(updatedPs, el.locallyFree, el.connectiveUsed))

      case ETupleBody(el) =>
        for {
          evaledPs  <- el.ps.toList.traverse(evalExpr)
          updatedPs = evaledPs.map(updateLocallyFree)
        } yield updateLocallyFree(ETuple(updatedPs, el.locallyFree, el.connectiveUsed))

      case ESetBody(set) =>
        for {
          evaledPs  <- set.ps.sortedPars.traverse(evalExpr)
          updatedPs = evaledPs.map(updateLocallyFree)
        } yield set.copy(ps = SortedParHashSet(updatedPs))

      case EMapBody(map) =>
        for {
          evaledPs <- map.ps.sortedList.traverse {
                       case (key, value) =>
                         for {
                           eKey   <- evalExpr(key).map(updateLocallyFree)
                           eValue <- evalExpr(value).map(updateLocallyFree)
                         } yield (eKey, eValue)
                     }
        } yield map.copy(ps = SortedParMap(evaledPs))

      case EMethodBody(EMethod(method, target, arguments, _, _)) =>
        for {
          _            <- charge[M](METHOD_CALL_COST)
          evaledTarget <- evalExpr(target)
          evaledArgs   <- arguments.toList.traverse(evalExpr)
          resultPar <- methodTable.get(method) match {
                        case None =>
                          ReduceError("Unimplemented method: " + method).raiseError[M, Par]
                        case Some(f) => f(evaledTarget, evaledArgs)
                      }
          resultExpr <- evalSingleExpr(resultPar)
        } yield resultExpr
      case _ => ReduceError("Unimplemented expression: " + expr).raiseError[M, Expr]
    }
  }

  private abstract class Method() {
    def apply(p: Par, args: Seq[Par])(
        implicit env: Env[Par]
    ): M[Par]
  }

  private[this] val nth: Method = new Method() {

    def localNth(ps: Seq[Par], nth: Int): Either[ReduceError, Par] =
      if (ps.isDefinedAt(nth)) ps(nth).asRight[ReduceError]
      else ReduceError("Error: index out of bound: " + nth).asLeft[Par]

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      if (args.length != 1)
        errors.MethodArgumentNumberMismatch("nth", 1, args.length).raiseError[M, Par]
      else {
        for {
          nthRaw <- evalToLong(args.head)
          nth    <- restrictToInt(nthRaw)
          v      <- evalSingleExpr(p)
          result <- v.exprInstance match {
                     case EListBody(EList(ps, _, _, _)) =>
                       s.fromEither(localNth(ps, nth))
                     case ETupleBody(ETuple(ps, _, _)) =>
                       s.fromEither(localNth(ps, nth))
                     case GByteArray(bs) =>
                       s.fromEither(if (0 <= nth && nth < bs.size) {
                         val b      = bs.byteAt(nth) & 0xff // convert to unsigned
                         val p: Par = Expr(GInt(b.toLong))
                         p.asRight[ReduceError]
                       } else ReduceError("Error: index out of bound: " + nth).asLeft[Par])
                     case _ =>
                       ReduceError("Error: nth applied to something that wasn't a list or tuple.")
                         .raiseError[M, Par]
                   }
          _ <- charge[M](NTH_METHOD_CALL_COST)
        } yield result
      }
  }

  private[this] val toByteArray: Method = new Method() {

    def serialize(p: Par): Either[ReduceError, Array[Byte]] =
      Either
        .fromTry(Try(Serialize[Par].encode(p).toArray))
        .leftMap(th => ReduceError(s"Error: exception thrown when serializing $p." + th.getMessage))

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      if (args.nonEmpty)
        MethodArgumentNumberMismatch("toByteArray", 0, args.length).raiseError[M, Par]
      else {
        for {
          exprEvaled <- evalExpr(p)
          exprSubst  <- substituteAndCharge[Par, M](exprEvaled, 0, env)
          _          <- charge[M](toByteArrayCost(exprSubst))
          ba         <- s.fromEither(serialize(exprSubst))
        } yield Expr(GByteArray(ByteString.copyFrom(ba)))
      }
  }

  private[this] val hexToBytes: Method = new Method() {

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      if (args.nonEmpty)
        MethodArgumentNumberMismatch("hexToBytes", 0, args.length).raiseError[M, Par]
      else {
        p.singleExpr() match {
          case Some(Expr(GString(encoded))) =>
            for {
              _ <- charge[M](hexToBytesCost(encoded))
              res <- Try(ByteString.copyFrom(Base16.unsafeDecode(encoded))).fold(
                      th =>
                        ReduceError(
                          s"Error: exception was thrown when decoding input string to hexadecimal: ${th.getMessage}"
                        ).raiseError[M, Par],
                      ba => (Expr(GByteArray(ba)): Par).pure[M]
                    )
            } yield res
          case Some(Expr(other)) =>
            MethodNotDefined("hexToBytes", other.typ).raiseError[M, Par]
          case None =>
            ReduceError("Error: Method can only be called on singular expressions.")
              .raiseError[M, Par]
        }
      }
  }

  private[this] val toUtf8Bytes: Method = new Method() {

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      if (args.nonEmpty)
        MethodArgumentNumberMismatch("toUtf8Bytes", 0, args.length).raiseError[M, Par]
      else {
        p.singleExpr() match {
          case Some(Expr(GString(utf8string))) =>
            charge[M](hexToBytesCost(utf8string)) >>
              (GByteArray(ByteString.copyFrom(utf8string.getBytes("UTF-8"))): Par).pure[M]
          case Some(Expr(other)) =>
            MethodNotDefined("toUtf8Bytes", other.typ).raiseError[M, Par]
          case None =>
            ReduceError("Error: Method can only be called on singular expressions.")
              .raiseError[M, Par]
        }
      }
  }

  private[this] val union: Method = new Method() {

    def locallyFreeUnion(base: Coeval[BitSet], other: Coeval[BitSet]): Coeval[BitSet] =
      base.flatMap(b => other.map(o => b | o))

    def union(baseExpr: Expr, otherExpr: Expr): M[Expr] =
      (baseExpr.exprInstance, otherExpr.exprInstance) match {
        case (
            ESetBody(base @ ParSet(basePs, _, _, _)),
            ESetBody(other @ ParSet(otherPs, _, _, _))
            ) =>
          charge[M](unionCost(otherPs.size)) >>
            Expr(
              ESetBody(
                ParSet(
                  basePs.union(otherPs.sortedPars.toSet),
                  base.connectiveUsed || other.connectiveUsed,
                  locallyFreeUnion(base.locallyFree, other.locallyFree),
                  None
                )
              )
            ).pure[M]
        case (
            EMapBody(base @ ParMap(baseMap, _, _, _)),
            EMapBody(other @ ParMap(otherMap, _, _, _))
            ) =>
          charge[M](unionCost(otherMap.size)) >>
            Expr(
              EMapBody(
                ParMap(
                  (baseMap ++ otherMap).toSeq,
                  base.connectiveUsed || other.connectiveUsed,
                  locallyFreeUnion(base.locallyFree, other.locallyFree),
                  None
                )
              )
            ).pure[M]

        case (other, _) => MethodNotDefined("union", other.typ).raiseError[M, Expr]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.length != 1)
              MethodArgumentNumberMismatch("union", 1, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr  <- evalSingleExpr(p)
        otherExpr <- evalSingleExpr(args.head)
        result    <- union(baseExpr, otherExpr)
      } yield result
  }

  private[this] val diff: Method = new Method() {

    def diff(baseExpr: Expr, otherExpr: Expr): M[Expr] =
      (baseExpr.exprInstance, otherExpr.exprInstance) match {
        case (ESetBody(ParSet(basePs, _, _, _)), ESetBody(ParSet(otherPs, _, _, _))) =>
          // diff is implemented in terms of foldLeft that at each step
          // removes one element from the collection.
          charge[M](diffCost(otherPs.size)) >>
            Expr(ESetBody(ParSet(basePs.sortedPars.toSet.diff(otherPs.sortedPars.toSet).toSeq)))
              .pure[M]
        case (EMapBody(ParMap(basePs, _, _, _)), EMapBody(ParMap(otherPs, _, _, _))) =>
          charge[M](diffCost(otherPs.size)) >>
            Expr(EMapBody(ParMap(basePs -- otherPs.keys))).pure[M]
        case (other, _) =>
          MethodNotDefined("diff", other.typ).raiseError[M, Expr]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.length != 1)
              MethodArgumentNumberMismatch("diff", 1, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr  <- evalSingleExpr(p)
        otherExpr <- evalSingleExpr(args.head)
        result    <- diff(baseExpr, otherExpr)
      } yield result
  }

  private[this] val add: Method = new Method() {

    def add(baseExpr: Expr, par: Par): M[Expr] =
      baseExpr.exprInstance match {
        case ESetBody(base @ ParSet(basePs, _, _, _)) =>
          Expr(
            ESetBody(
              ParSet(
                basePs + par,
                base.connectiveUsed || par.connectiveUsed,
                base.locallyFree.map(b => b | par.locallyFree),
                None
              )
            )
          ).pure[M]
        //TODO(mateusz.gorski): think whether cost accounting for addition should be dependend on the operands
        case other => MethodNotDefined("add", other.typ).raiseError[M, Expr]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.length != 1)
              MethodArgumentNumberMismatch("add", 1, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr <- evalSingleExpr(p)
        element  <- evalExpr(args.head)
        result   <- add(baseExpr, element)
        _        <- charge[M](ADD_COST)
      } yield result
  }

  private[this] val delete: Method = new Method() {

    def delete(baseExpr: Expr, par: Par): M[Expr] =
      baseExpr.exprInstance match {
        case ESetBody(base @ ParSet(basePs, _, _, _)) =>
          Expr(
            ESetBody(
              ParSet(
                basePs - par,
                base.connectiveUsed || par.connectiveUsed,
                base.locallyFree.map(b => b | par.locallyFree),
                None
              )
            )
          ).pure[M]
        case EMapBody(base @ ParMap(basePs, _, _, _)) =>
          Expr(
            EMapBody(
              ParMap(
                basePs - par,
                base.connectiveUsed || par.connectiveUsed,
                base.locallyFree.map(b => b | par.locallyFree),
                None
              )
            )
          ).pure[M]
        case other =>
          MethodNotDefined("delete", other.typ).raiseError[M, Expr]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.length != 1)
              MethodArgumentNumberMismatch("delete", 1, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr <- evalSingleExpr(p)
        element  <- evalExpr(args.head)
        result   <- delete(baseExpr, element)
        _        <- charge[M](REMOVE_COST) //TODO(mateusz.gorski): think whether deletion of an element from the collection should dependent on the collection type/size
      } yield result
  }

  private[this] val contains: Method = new Method() {

    def contains(baseExpr: Expr, par: Par): M[Expr] =
      baseExpr.exprInstance match {
        case ESetBody(ParSet(basePs, _, _, _)) =>
          (GBool(basePs.contains(par)): Expr).pure[M]
        case EMapBody(ParMap(basePs, _, _, _)) =>
          (GBool(basePs.contains(par)): Expr).pure[M]
        case other => MethodNotDefined("contains", other.typ).raiseError[M, Expr]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.length != 1)
              MethodArgumentNumberMismatch("contains", 1, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr <- evalSingleExpr(p)
        element  <- evalExpr(args.head)
        result   <- contains(baseExpr, element)
        _        <- charge[M](LOOKUP_COST)
      } yield result
  }

  private[this] val get: Method = new Method() {

    def get(baseExpr: Expr, key: Par): M[Par] =
      baseExpr.exprInstance match {
        case EMapBody(ParMap(basePs, _, _, _)) =>
          basePs.getOrElse(key, VectorPar()).pure[M]
        case other => MethodNotDefined("get", other.typ).raiseError[M, Par]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.length != 1)
              MethodArgumentNumberMismatch("get", 1, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr <- evalSingleExpr(p)
        key      <- evalExpr(args.head)
        result   <- get(baseExpr, key)
        _        <- charge[M](LOOKUP_COST)
      } yield result
  }

  private[this] val getOrElse: Method = new Method {

    def getOrElse(baseExpr: Expr, key: Par, default: Par): M[Par] =
      baseExpr.exprInstance match {
        case EMapBody(ParMap(basePs, _, _, _)) =>
          basePs.getOrElse(key, default).pure[M]
        case other => MethodNotDefined("getOrElse", other.typ).raiseError[M, Par]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.length != 2)
              MethodArgumentNumberMismatch("getOrElse", 2, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr <- evalSingleExpr(p)
        key      <- evalExpr(args.head)
        default  <- evalExpr(args(1))
        result   <- getOrElse(baseExpr, key, default)
        _        <- charge[M](LOOKUP_COST)
      } yield result
  }

  private[this] val set: Method = new Method() {

    def set(baseExpr: Expr, key: Par, value: Par): M[Par] =
      baseExpr.exprInstance match {
        case EMapBody(ParMap(basePs, _, _, _)) =>
          (ParMap(basePs + (key -> value)): Par).pure[M]
        case other => MethodNotDefined("set", other.typ).raiseError[M, Par]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.length != 2)
              MethodArgumentNumberMismatch("set", 2, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr <- evalSingleExpr(p)
        key      <- evalExpr(args.head)
        value    <- evalExpr(args(1))
        result   <- set(baseExpr, key, value)
        _        <- charge[M](ADD_COST)
      } yield result
  }

  private[this] val keys: Method = new Method() {

    def keys(baseExpr: Expr): M[Par] =
      baseExpr.exprInstance match {
        case EMapBody(ParMap(basePs, _, _, _)) =>
          (ParSet(basePs.keys.toSeq): Par).pure[M]
        case other =>
          MethodNotDefined("keys", other.typ).raiseError[M, Par]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.nonEmpty)
              MethodArgumentNumberMismatch("keys", 0, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr <- evalSingleExpr(p)
        result   <- keys(baseExpr)
        _        <- charge[M](KEYS_METHOD_COST)
      } yield result
  }

  private[this] val size: Method = new Method() {

    def size(baseExpr: Expr): M[(Int, Par)] =
      baseExpr.exprInstance match {
        case EMapBody(ParMap(basePs, _, _, _)) =>
          val size = basePs.size
          (size, GInt(size.toLong): Par).pure[M]
        case ESetBody(ParSet(ps, _, _, _)) =>
          val size = ps.size
          (size, GInt(size.toLong): Par).pure[M]
        case other =>
          MethodNotDefined("size", other.typ).raiseError[M, (Int, Par)]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.nonEmpty)
              MethodArgumentNumberMismatch("size", 0, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr <- evalSingleExpr(p)
        result   <- size(baseExpr)
        _        <- charge[M](sizeMethodCost(result._1))
      } yield result._2
  }

  private[this] val length: Method = new Method() {

    def length(baseExpr: Expr): M[Expr] =
      baseExpr.exprInstance match {
        case GString(string) =>
          (GInt(string.length.toLong): Expr).pure[M]
        case GByteArray(bytes) =>
          (GInt(bytes.size.toLong): Expr).pure[M]
        case EListBody(EList(ps, _, _, _)) =>
          (GInt(ps.length.toLong): Expr).pure[M]
        case other =>
          MethodNotDefined("length", other.typ).raiseError[M, Expr]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.nonEmpty)
              MethodArgumentNumberMismatch("length", 0, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr <- evalSingleExpr(p)
        result   <- length(baseExpr)
        _        <- charge[M](LENGTH_METHOD_COST)
      } yield result
  }

  private[this] val slice: Method = new Method() {

    def slice(baseExpr: Expr, from: Int, until: Int): M[Par] =
      baseExpr.exprInstance match {
        case GString(string) =>
          s.delay(GString(string.slice(from, until)))
        case EListBody(EList(ps, locallyFree, connectiveUsed, remainder)) =>
          s.delay(EList(ps.slice(from, until), locallyFree, connectiveUsed, remainder))
        case GByteArray(bytes) =>
          s.delay(GByteArray(bytes.substring(from, until)))
        case other =>
          MethodNotDefined("slice", other.typ).raiseError[M, Par]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.length != 2)
              MethodArgumentNumberMismatch("slice", 2, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr   <- evalSingleExpr(p)
        fromArgRaw <- evalToLong(args.head)
        fromArg    <- restrictToInt(fromArgRaw)
        toArgRaw   <- evalToLong(args(1))
        toArg      <- restrictToInt(toArgRaw)
        result     <- slice(baseExpr, fromArg, toArg)
        _          <- charge[M](sliceCost(toArg))
      } yield result
  }

  private[this] val toList: Method = new Method() {

    def toList(baseExpr: Expr): M[Par] =
      baseExpr.exprInstance match {
        case e: EListBody =>
          (e: Par).pure[M]
        case ESetBody(ParSet(ps, _, _, _)) =>
          charge[M](toListCost(ps.size)) >>
            (EList(ps.toList): Par).pure[M]
        case EMapBody(ParMap(ps, _, _, _)) =>
          charge[M](toListCost(ps.size)) >>
            (EList(
              ps.toSeq.map {
                case (k, v) =>
                  Par().withExprs(Seq(Expr(ETupleBody(ETuple(Seq(k, v))))))
              }
            ): Par).pure[M]
        case ETupleBody(ETuple(ps, _, _)) =>
          charge[M](toListCost(ps.size)) >>
            (EList(ps.toList): Par).pure[M]
        case other =>
          MethodNotDefined("toList", other.typ).raiseError[M, Par]
      }

    override def apply(p: Par, args: Seq[Par])(implicit env: Env[Par]): M[Par] =
      for {
        _ <- if (args.nonEmpty)
              MethodArgumentNumberMismatch("toList", 0, args.length).raiseError[M, Unit]
            else ().pure[M]
        baseExpr <- evalSingleExpr(p)
        result   <- toList(baseExpr)
      } yield result
  }

  private val methodTable: Map[String, Method] =
    Map(
      "nth"         -> nth,
      "toByteArray" -> toByteArray,
      "hexToBytes"  -> hexToBytes,
      "toUtf8Bytes" -> toUtf8Bytes,
      "union"       -> union,
      "diff"        -> diff,
      "add"         -> add,
      "delete"      -> delete,
      "contains"    -> contains,
      "get"         -> get,
      "getOrElse"   -> getOrElse,
      "set"         -> set,
      "keys"        -> keys,
      "size"        -> size,
      "length"      -> length,
      "slice"       -> slice,
      "toList"      -> toList
    )

  def evalSingleExpr(p: Par)(implicit env: Env[Par]): M[Expr] =
    if (p.sends.nonEmpty || p.receives.nonEmpty || p.news.nonEmpty || p.matches.nonEmpty || p.unforgeables.nonEmpty || p.bundles.nonEmpty)
      ReduceError("Error: parallel or non expression found where expression expected.")
        .raiseError[M, Expr]
    else
      p.exprs match {
        case (e: Expr) +: Nil => evalExprToExpr(e)
        case _                => ReduceError("Error: Multiple expressions given.").raiseError[M, Expr]
      }

  def evalToLong(
      p: Par
  )(implicit env: Env[Par]): M[Long] =
    if (p.sends.nonEmpty || p.receives.nonEmpty || p.news.nonEmpty || p.matches.nonEmpty || p.unforgeables.nonEmpty || p.bundles.nonEmpty)
      ReduceError("Error: parallel or non expression found where expression expected.")
        .raiseError[M, Long]
    else
      p.exprs match {
        case Expr(GInt(v)) +: Nil =>
          (v: Long).pure[M]
        case Expr(EVarBody(EVar(v))) +: Nil =>
          for {
            p      <- eval(v)
            intVal <- evalToLong(p)
          } yield intVal
        case (e: Expr) +: Nil =>
          for {
            evaled <- evalExprToExpr(e)
            result <- evaled.exprInstance match {
                       case GInt(v) =>
                         (v: Long).pure[M]
                       case _ =>
                         ReduceError("Error: expression didn't evaluate to integer.")
                           .raiseError[M, Long]
                     }
          } yield result
        case _ =>
          ReduceError("Error: Integer expected, or unimplemented expression.").raiseError[M, Long]
      }

  def evalToBool(
      p: Par
  )(implicit env: Env[Par]): M[Boolean] =
    if (p.sends.nonEmpty || p.receives.nonEmpty || p.news.nonEmpty || p.matches.nonEmpty || p.unforgeables.nonEmpty || p.bundles.nonEmpty)
      ReduceError("Error: parallel or non expression found where expression expected.")
        .raiseError[M, Boolean]
    else
      p.exprs match {
        case Expr(GBool(b)) +: Nil =>
          (b: Boolean).pure[M]
        case Expr(EVarBody(EVar(v))) +: Nil =>
          for {
            p       <- eval(v)
            boolVal <- evalToBool(p)
          } yield boolVal
        case (e: Expr) +: Nil =>
          for {
            evaled <- evalExprToExpr(e)
            result <- evaled.exprInstance match {
                       case GBool(b) => (b: Boolean).pure[M]
                       case _ =>
                         ReduceError("Error: expression didn't evaluate to boolean.")
                           .raiseError[M, Boolean]
                     }
          } yield result
        case _ => ReduceError("Error: Multiple expressions given.").raiseError[M, Boolean]
      }

  private def restrictToInt(long: Long): M[Int] =
    s.catchNonFatal(Math.toIntExact(long)).adaptError {
      case _: ArithmeticException => ReduceError(s"Integer overflow for value $long")
    }

  private def updateLocallyFree(par: Par): Par =
    par.copy(
      locallyFree = par.sends.foldLeft(BitSet())((acc, send) => acc | send.locallyFree) |
        par.receives.foldLeft(BitSet())((acc, receive) => acc | receive.locallyFree) |
        par.news.foldLeft(BitSet())((acc, newProc) => acc | newProc.locallyFree) |
        par.exprs.foldLeft(BitSet())((acc, expr) => acc | ExprLocallyFree.locallyFree(expr, 0)) |
        par.matches.foldLeft(BitSet())((acc, matchProc) => acc | matchProc.locallyFree) |
        par.bundles.foldLeft(BitSet())((acc, bundleProc) => acc | bundleProc.locallyFree)
    )

  private def updateLocallyFree(elist: EList): EList =
    elist.copy(locallyFree = elist.ps.foldLeft(BitSet())((acc, p) => acc | p.locallyFree))

  private def updateLocallyFree(elist: ETuple): ETuple =
    elist.copy(locallyFree = elist.ps.foldLeft(BitSet())((acc, p) => acc | p.locallyFree))

  /**
    * Evaluate any top level expressions in @param Par .
    */
  def evalExpr(par: Par)(implicit env: Env[Par]): M[Par] =
    for {
      evaledExprs <- par.exprs.toList.traverse(evalExprToPar)
      // Note: the locallyFree cache in par could now be invalid, but given
      // that locallyFree is for use in the matcher, and the matcher uses
      // substitution, it will resolve in that case. AlwaysEqual makes sure
      // that this isn't an issue in the rest of cases.
      result = evaledExprs.foldLeft(par.copy(exprs = Vector())) { (acc, newPar) =>
        acc ++ newPar
      }
    } yield result
}

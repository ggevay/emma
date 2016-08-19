package eu.stratosphere.emma
package compiler

import api.DataBag
import lang.TreeMatchers

import shapeless._

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FreeSpec, Matchers}

/**
 * Common methods and mixins for all compier specs
 */
trait BaseCompilerSpec extends FreeSpec with Matchers with PropertyChecks with TreeMatchers {

  val compiler = new RuntimeCompiler()

  import compiler._
  import universe._
  import Core.{Lang => core}

  // ---------------------------------------------------------------------------
  // Common transformation pipelines
  // ---------------------------------------------------------------------------

  def typeCheck[T]: Expr[T] => Tree = {
    (_: Expr[T]).tree
  } andThen {
    Type.check(_: Tree)
  }

  /** Apply after [[Core.anf()]] for testing purposes. */
  lazy val normalizeLet: u.Tree => u.Tree =
    api.BottomUp.withParent.transformWith {
      case Attr.inh(core.Let(Seq(), Seq(), expr), core.Branch(_, _, _) :: _) => expr
      case Attr.inh(let @ core.Let(_, _, _), core.DefDef(_, _, _, _, _) :: _) => let
      case Attr.inh(core.Atomic(expr), core.DefDef(_, _, _, _, _) :: _) =>
        core.Let()()(expr).asInstanceOf[u.Tree]
    }.andThen(_.tree)

  // ---------------------------------------------------------------------------
  // Common value definitions used in compiler tests
  // ---------------------------------------------------------------------------

  val x = 42
  val y = "The answer to life, the universe and everything"
  val t = (x, y)
  val xs = DataBag(Seq(1, 2, 3))
  val ys = DataBag(Seq(1, 2, 3))

  // ---------------------------------------------------------------------------
  // Utility functions
  // ---------------------------------------------------------------------------

  def time[A](f: => A, name: String = "") = {
    val s = System.nanoTime
    val ret = f
    println(s"$name time: ${(System.nanoTime - s) / 1e6}ms".trim)
    ret
  }
}

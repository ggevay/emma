package eu.stratosphere.emma
package compiler.lang.core

import compiler.BaseCompilerSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `LNF.cse` transformation. */
@RunWith(classOf[JUnitRunner])
class CSESpec extends BaseCompilerSpec {

  import compiler._
  import universe._

  def typeCheckAndNormalize[T]: Expr[T] => Tree = {
    (_: Expr[T]).tree
  } andThen {
    Type.check(_)
  } andThen {
    Core.destructPatternMatches
  } andThen {
    Core.resolveNameClashes
  } andThen {
    Core.anf
  } andThen {
    time(Core.cse(_), "cse")
  } andThen {
    Owner.at(Owner.enclosing)
  }

  "field selections" - {

    "as argument" in {
      val act = typeCheckAndNormalize(reify {
        15 * t._1
      })

      val exp = typeCheck(reify {
        val x$1 = t
        val x$2 = x$1._1
        val x$3 = 15 * x$2
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "as selection" in {
      val act = typeCheckAndNormalize(reify {
        t._1 * 15
      })

      val exp = typeCheck(reify {
        val x$1 = t
        val x$2 = x$1._1
        val x$3 = x$2 * 15
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "package selections" in {
      val act = typeCheckAndNormalize(reify {
        val bag = eu.stratosphere.emma.api.DataBag(Seq(1, 2, 3))
        scala.Predef.println(bag.fetch())
      })

      val exp = typeCheck(reify {
        val x$1 = Seq(1, 2, 3)
        val bag = eu.stratosphere.emma.api.DataBag(x$1)
        val x$2 = bag.fetch()
        val x$3 = scala.Predef.println(x$2)
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "complex arguments" - {
    "lhs" in {
      val act = typeCheckAndNormalize(reify {
        y.substring(y.indexOf('l') + 1)
      })

      val exp = typeCheckAndNormalize(reify {
        val y$1 = y
        val x$1 = y$1.indexOf('l')
        val x$2 = x$1 + 1
        val x$3 = y$1.substring(x$2)
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "nested blocks" in {
    val act = typeCheckAndNormalize(reify {
      val z = y
      val a = {
        val b = y.indexOf('a')
        b + 15
      }
      val c = {
        val b = z.indexOf('a')
        b + 15
      }
      a + c
    })

    val exp = typeCheck(reify {
      val y$1 = y
      val b$1 = y$1.indexOf('a')
      val a = b$1 + 15
      val r = a + a
      r
    })

    act shouldBe alphaEqTo(exp)
  }

  "constant propagation" in {
    val act = typeCheckAndNormalize(reify {
      val a = 42
      val b = a
      val c = b
      c
    })

    val exp = typeCheck(reify {
      42
    })

    act shouldBe alphaEqTo(exp)
  }
}

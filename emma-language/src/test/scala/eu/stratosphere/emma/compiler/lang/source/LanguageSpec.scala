package eu.stratosphere
package emma.compiler
package lang
package source

import emma.api.DataBag
import emma.testschema.Marketing._

import org.example.foo.{Bar, Baz}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec defining the core fragment of Scala supported by Emma. */
@RunWith(classOf[JUnitRunner])
class LanguageSpec extends BaseCompilerSpec {

  import compiler._
  import universe._
  import Source.{Lang => src}
  import Source.valid
  import Validation._

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  /** Example pre-processing pipeline. */
  lazy val pipeline = { api.Type.check(_: u.Tree) }
    .andThen(compiler.fixLambdaTypes)
    .andThen(compiler.unQualifyStaticModules)
    .andThen(compiler.normalizeStatements)

  /** Extracts examples from a reified expression. */
  def extractFrom[A](expr: u.Expr[A]) =
    pipeline(expr.tree) match {
      case u.Apply(_, args) => args
      case u.Block(stats, _) => stats
    }

  /** Tree [[Validator]] matcher. */
  def satisfy(validator: Validator) =
     be (good) compose { (tree: u.Tree) =>
      time(validateAs(validator, tree), "validate")
    }

  override def alphaEqTo[T <: u.Tree](tree: T) =
    super.alphaEqTo(api.Owner.at(get.enclosingOwner)(tree))

  // ---------------------------------------------------------------------------
  // Atomics
  // ---------------------------------------------------------------------------

  "Literals" - {
    // modeled by
    // - `Lit` objects in `Source.Lang`
    // - `Literal(Constant(value))` nodes in Scala ASTs

    val examples = extractFrom(reify(42, 4.2, "42", '!'))
    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Lit)
    }

    "can be destructed and constructed" in {
      examples foreach { case x @ src.Lit(const) =>
        x shouldBe alphaEqTo (src.Lit(const))
      }
    }
  }

  "References" - {
    // modeled by
    // - `Ref` objects in `Source.Lang`
    // - `Ident(sym)` nodes where `sym` is a (free) `TermSymbol` in Scala ASTs

    val examples = extractFrom(reify {
      val u = 42
      val v = 4.2
      var w = "42"
      object Module
      w += '!'
      (u, v, w, Module, scala.collection.Seq, emma.util.Monoids)
      ()
    }).last.children.tail

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Ref)
    }

    "can be destructed and constructed" in {
      examples foreach { case x @ src.Ref(sym) =>
        x shouldBe alphaEqTo (src.Ref(sym))
      }
    }
  }

  "This references" - {
    // modeled by
    // - `This` objects in `Source.Lang`
    // - `This(qual)` nodes in Scala ASTs

    val examples = extractFrom(reify {
      class Unqualified { println(this.toString) }
      class Qualified { println(LanguageSpec.this.x) }
      object Module { println(this.hashCode) }
      Module: AnyRef
    }).map(_.collect {
      case ths: u.This => ths
    }.head)

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.This)
    }

    "can be destructed and constructed" in {
      examples foreach { case x @ src.This(sym) =>
        x shouldBe alphaEqTo (src.This(sym))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Values and variables
  // ---------------------------------------------------------------------------

  "Value and variable definitions" - {
    // modeled by
    // - `BindingDef` objects in `Source.Lang`
    // - `ValDef(lhs, rhs)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify {
      val u = s"$x is $y"
      val v = 42
      var w = "42"
      w += '!'
      (u, v, w)
    }).collect {
      case vdf: u.ValDef => vdf
    }

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.BindingDef)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.BindingDef(lhs, rhs, flags) =>
        x shouldBe alphaEqTo (src.BindingDef(lhs, rhs, flags))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Methods
  // ---------------------------------------------------------------------------

  "Method calls" - {
    // modeled by
    // - `DefCall` objects in `Source.Lang`
    // - `Apply(fun, args)` nodes in Scala ASTs

    val examples = extractFrom(reify(
      t._2,
      DEFAULT_CLASS,
      x == 42,
      scala.Predef.println(y),
      y.substring(1),
      ((x: Int, y: Int) => x + y) (x, x),
      Seq(x, x),
      DataBag(xs.fetch()),
      List.canBuildFrom[Int],
      DataBag(Seq(1, 2, 3)).sum
    )).map(api.Tree.unAscribe)

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.DefCall)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.DefCall(target, method, targs, argss@_*) =>
        x shouldBe alphaEqTo (src.DefCall(target)(method, targs: _*)(argss: _*))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Loops
  // ---------------------------------------------------------------------------

  "While loops" - {
    // modeled by
    // - `While` objects in `Source.Lang`
    // - `LabelDef(...)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify {
      var r = 0
      var i = 0
      while (i < x) {
        i = i + 1
        r = r * i
      }
      i * r
    }).collect {
      case loop: u.LabelDef => loop
    }

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.While)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.While(cond, body) =>
        x shouldBe alphaEqTo (src.While(cond, body))
      }
    }
  }

  "Do-while loops" - {
    // modeled by
    // - `DoWhile` objects in `Source.Lang`
    // - `LabelDef(...)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify {
      var i = 0
      var r = 0
      do {
        i = i + 1
        r = r * i
      } while (i < x)
      i * r
    }).collect {
      case loop: u.LabelDef => loop
    }

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.DoWhile)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.DoWhile(cond, body) =>
        x shouldBe alphaEqTo (src.DoWhile(cond, body))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Patters
  // ---------------------------------------------------------------------------

  "Pattern matches" - {
    // modeled by
    // - `PatMat` objects in `Source.Lang`
    // - `Match(selector, cases)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify(
      ((1, 2): Any) match {
        case (x: Int, _) => x
        case Ad(id, name, _) => id
        case Click(adID, userID, time) => adID
        case _ => 42
      },
      "binding" match {
        case s@(_: String) => s + t._2
      }
    ))

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.PatMat)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.PatMat(target, cases@_*) =>
        x shouldBe alphaEqTo (src.PatMat(target, cases.map {
          case src.PatCase(pat, guard, body) => src.PatCase(pat, guard, body)
        }: _*))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Terms
  // ---------------------------------------------------------------------------

  "Blocks" - {
    // modeled by
    // - `Block` objects in `Source.Lang`
    // - `Block(stats, expr)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify(
      { val z = 5; x + z },
      t._2 + "implicit unit": Unit
    )).map(api.Tree.unAscribe)

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Block)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.Block(stats, expr) =>
        x shouldBe alphaEqTo (src.Block(stats: _*)(expr))
      }
    }
  }

  "Branches" - {
    // modeled by
    // - `Branch` objects in `Source.Lang`
    // - `If(cond, thn, els)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify(
      if (x == 42) x else x / 42,
      if (x < 42) "only one branch"
    ))

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Branch)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.Branch(cond, thn, els) =>
        x shouldBe alphaEqTo (src.Branch(cond, thn, els))
      }
    }
  }

  "Class instantiations" - {
    // modeled by
    // - `Inst` objects in `Source.Lang`
    // - `Apply(tpt: New, _)` nodes in Scala ASTs

    val examples = extractFrom(reify(
      new Ad(1, "Uber AD", AdClass.SERVICES),
      new Baz(x),
      new Bar[Int](x)
    ))

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Inst)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.Inst(clazz, targs, argss@_*) =>
        x shouldBe alphaEqTo (src.Inst(clazz, targs: _*)(argss: _*))
      }
    }
  }

  "Lambdas" - {
    // modeled by
    // - `Lambda` objects in `Source.Lang`
    // - `Function(args, body)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify(
      (x: Int, y: Int) => x + y,
      "ellipsis".charAt _
    )).flatMap {
      case u.Block(stats, _) => stats
      case tree => Seq(tree)
    }

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Lambda)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.Lambda(_, params, body) =>
        x shouldBe alphaEqTo (src.Lambda(params.map(api.TermSym.of(_)): _*)(body))
      }
    }
  }

  "Type ascriptions" - {
    // modeled by
    // - `inst` objects in `Source.Language`
    // - `Typed(expr, tpt)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify(
      x: Number,
      t: (Any, String)
    ))

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.TypeAscr)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.TypeAscr(tree, tpe) =>
        x shouldBe alphaEqTo (src.TypeAscr(tree, tpe))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Statements
  // ---------------------------------------------------------------------------

  "Variable mutations" - {
    // modeled by
    // - `val_` objects in `Source.Language`
    // - `Assign(lhs, rhs)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify {
      var u = "still a ValDef but mutable"
      var w = 42
      u = "an updated ValDef"
      w = w + 1
    }).collect {
      case mut: u.Assign => mut
    }

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.VarMut)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ src.VarMut(lhs, rhs) =>
        x shouldBe alphaEqTo (src.VarMut(lhs, rhs))
      }
    }
  }
}

package eu.stratosphere.emma
package compiler.lang.core

import compiler.lang.source.Source
import compiler.Common
import util.Monoids

import shapeless._

import scala.annotation.tailrec

/** Administrative Normal Form (ANF) bypassing control-flow and for-comprehensions. */
private[core] trait ANF extends Common {
  this: Source with Core =>

  import UniverseImplicits._
  import Core.{Lang => core}
  import Source.{Lang => src}

  /** Administrative Normal Form (ANF) bypassing control-flow and for-comprehensions. */
  private[core] object ANF {

    /** Ensures that all definitions within `tree` have unique names. */
    def resolveNameClashes(tree: u.Tree): u.Tree =
      api.Tree.refresh(nameClashes(tree): _*)(tree)

    /** The ANF transformation. */
    private lazy val anfTransform: u.Tree => u.Tree =
      api.BottomUp.withParent.inherit {
        case tree => is.tpe(tree)
      } (Monoids.disj).withOwner.transformWith {
        // Bypass type-trees
        case Attr.inh(tree, _ :: true :: _) =>
          tree

        // Bypass atomics (except in lambdas and comprehensions)
        case Attr.inh(src.Atomic(atom), _ :: _ :: parent :: _) => parent match {
          case src.Lambda(_, _, _) => src.Block()(atom)
          case Comprehension(_) => src.Block()(atom)
          case _ => atom
        }

        // Bypass parameters
        case Attr.none(param @ src.ParDef(_, _, _)) =>
          param

        // Bypass comprehensions
        case Attr.none(comprehension @ Comprehension(_)) =>
          src.Block()(comprehension)

        // Simplify RHS
        case Attr.none(src.VarMut(lhs, rhs)) =>
          val (stats, expr) = decompose(rhs, simplify = true)
          val mut = src.VarMut(lhs, expr)
          if (stats.isEmpty) mut
          else src.Block(stats :+ mut: _*)()

        // Simplify RHS
        case Attr.none(src.BindingDef(lhs, rhs, flags)) =>
          val (stats, expr) = decompose(rhs, simplify = true)
          val dfn = core.BindingDef(lhs, expr, flags)
          src.Block(stats :+ dfn: _*)()

        // Simplify expression
        case Attr.inh(src.TypeAscr(target, tpe), owner :: _) =>
          val (stats, expr) = decompose(target, simplify = false)
          val nme = api.TermName.fresh(nameOf(expr))
          val lhs = api.ValSym(owner, nme, tpe)
          val rhs = core.TypeAscr(expr, tpe)
          val dfn = core.ValDef(lhs, rhs)
          val ref = core.ValRef(lhs)
          src.Block(stats :+ dfn: _*)(ref)

        // Simplify target
        case Attr.inh(src.ModuleAcc(target, module) withType tpe, owner :: _) =>
          val (stats, expr) = decompose(target, simplify = false)
          val nme = api.TermName.fresh(module)
          val lhs = api.ValSym(owner, nme, tpe)
          val rhs = core.ModuleAcc(expr, module)
          val dfn = core.ValDef(lhs, rhs)
          val ref = core.ValRef(lhs)
          src.Block(stats :+ dfn: _*)(ref)

        // Simplify target & arguments
        case Attr.inh(src.DefCall(target, method, targs, argss@_*) withType tpe, owner :: _) =>
          val (tgtStats, tgtExpr) = target
            .map(decompose(_, simplify = false))
            .map { case (stats, expr) => (stats, Some(expr)) }
            .getOrElse(Seq.empty, None)

          val (argStats, argExprss) = decompose(argss, simplify = false)
          val nme = api.TermName.fresh(method)
          val lhs = api.ValSym(owner, nme, tpe)
          val rhs = core.DefCall(tgtExpr)(method, targs: _*)(argExprss: _*)
          val dfn = core.ValDef(lhs, rhs)
          val ref = core.ValRef(lhs)
          src.Block(tgtStats ++ argStats :+ dfn: _*)(ref)

        // Simplify arguments
        case Attr.inh(src.Inst(clazz, targs, argss@_*) withType tpe, owner :: _) =>
          val (stats, exprss) = decompose(argss, simplify = false)
          val nme = api.TermName.fresh(api.Sym.of(clazz))
          val lhs = api.ValSym(owner, nme, tpe)
          val rhs = core.Inst(clazz, targs: _*)(exprss: _*)
          val dfn = core.ValDef(lhs, rhs)
          val ref = core.ValRef(lhs)
          src.Block(stats :+ dfn: _*)(ref)

        // Flatten blocks
        case Attr.inh(src.Block(outer, expr), owner :: _) =>
          val (inner, result) = decompose(expr, simplify = false)
          val flat = outer.flatMap {
            case src.Block(stats, src.Atomic(_)) => stats
            case src.Block(stats, stat) => stats :+ stat
            case stat => Some(stat)
          }

          src.Block(flat ++ inner: _*)(result)

        // All lambdas on the RHS
        case Attr.none(lambda @ src.Lambda(fun, _, _) withType tpe) =>
          val nme = api.TermName.fresh(api.TermName.lambda)
          val lhs = api.ValSym(fun.owner, nme, tpe)
          val dfn = core.ValDef(lhs, lambda)
          val ref = core.ValRef(lhs)
          src.Block(dfn)(ref)

        // All branches on the RHS
        case Attr.inh(src.Branch(cond, thn, els) withType tpe, owner :: _) =>
          val (stats, expr) = decompose(cond, simplify = false)
          val branch = core.Branch(expr, thn, els)
          val nme = api.TermName.fresh("if")
          val lhs = api.ValSym(owner, nme, tpe)
          val dfn = core.ValDef(lhs, branch)
          val ref = core.ValRef(lhs)
          src.Block(stats :+ dfn: _*)(ref)
      }.andThen(_.tree)

    /**
     * Converts a tree into administrative normal form (ANF).
     *
     * == Preconditions ==
     *
     * - There are no name clashes (can be ensured with `resolveNameClashes`).
     *
     * == Postconditions ==
     *
     * - Introduces dedicated symbols for chains of length greater than one.
     * - Ensures that all function arguments are trivial identifiers.
     *
     * @param tree The tree to be converted.
     * @return An ANF version of the input tree.
     */
    def anf(tree: u.Tree): u.Tree = {
      lazy val clashes = nameClashes(tree)
      assert(clashes.isEmpty, s"Tree has name clashes:\n${clashes.mkString(", ")}")
      anfTransform(tree)
    }

    /**
     * Inlines `Ident` return expressions in blocks whenever referred symbol is used only once.
     * The resulting tree is said to be in ''simplified ANF'' form.
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[Core.anf()]]).
     *
     * == Postconditions ==
     * - `Ident` return expressions in blocks have been inlined whenever possible.
     */
    lazy val simplify: u.Tree => u.Tree =
      api.BottomUp.withValDefs.withValUses.transformWith {
        case Attr.syn(src.Block(stats, src.ValRef(target)), uses :: defs :: _)
          if defs.contains(target) && uses(target) == 1 =>
            val value = defs(target)
            src.Block(stats.filter(_ != value): _*)(value.rhs)
      }.andThen(_.tree)

    /**
     * Introduces `Ident` return expressions in blocks whenever the original expr is not a ref or
     * literal.The opposite of [[simplify]].
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[Core.anf()]]).
     *
     * == Postconditions ==
     * - `Ident` return expressions in blocks have been introduced whenever possible.
     */
    lazy val unsimplify: u.Tree => u.Tree =
      api.BottomUp.withOwner.transformWith {
        case Attr.none(let @ core.Let(_, _, core.Ref(_) | core.Lit(_))) => let
        case Attr.inh(core.Let(vals, defs, expr), owner :: _) =>
          val nme = api.TermName.fresh("x")
          val lhs = api.ValSym(owner, nme, expr.tpe)
          val ref = core.Ref(lhs)
          val dfn = core.ValDef(lhs, expr)
          core.Let(vals :+ dfn: _*)(defs: _*)(ref)
      }.andThen(_.tree)

    /**
     * Eliminates trivial type ascriptions.
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[Core.anf()]]).
     *
     * == Postconditions ==
     * - Trivial type ascriptions have been inlined.
     */
    lazy val removeTrivialTypeAscrs: u.Tree => u.Tree = tree => {
      val aliases = Map.newBuilder[u.TermSymbol, u.TermSymbol]

      val result = api.BottomUp.transform {
        case tree@core.TypeAscr(expr, tpe) if expr.tpe =:= tpe =>
          expr
        case tree@core.ValDef(lhs, core.Ref(rhs), _) =>
          aliases += lhs -> rhs
          tree
      }.andThen(_.tree)(tree)

      // compute closure of `aliases` map
      var rslt = Map.empty[u.TermSymbol, u.TermSymbol]
      var dlta = aliases.result()
      while (dlta.nonEmpty) {
        rslt = rslt ++ dlta
        dlta = for ((s1, s2) <- rslt; s3 <- rslt.get(s2)) yield s1 -> s3
      }

      api.BottomUp.transform {
        case tree@core.Ref(sym) =>
          // substitute aliasses
          core.Ref(rslt.getOrElse(sym, sym))
        case tree@core.Let(vals, defs, expr) =>
          // filter valdefs
          core.Let(vals filterNot {
            case core.ValDef(sym, _, _) => rslt.contains(sym)
            case _ => false
          }: _*)(defs: _*)(expr)
      }.andThen(_.tree)(result)
    }

    /**
     * Un-nests nested blocks.
     *
     * == Preconditions ==
     * - Except the nested blocks, the input tree is in simplified ANF form (see [[anf()]] and
     * [[simplify()]]).
     *
     * == Postconditions ==
     * - A simplified ANF tree where all nested blocks have been flattened.
     */
    lazy val flatten: u.Tree => u.Tree =
      api.BottomUp.transform {
        case parent @ src.Block(stats, expr) if hasNestedBlocks(parent) =>
          // Flatten (potentially) nested block expr
          val (exprStats, flatExpr) = decompose(expr, simplify = false)
          // Flatten (potentially) nested block stats
          val flatStats = stats.flatMap {
            case src.ValDef(lhs, src.Block(nestedStats, nestedExpr), flags) =>
              nestedStats :+ src.ValDef(lhs, nestedExpr, flags)
            case stat =>
              Seq(stat)
          }

          src.Block(flatStats ++ exprStats: _*)(flatExpr)
      }.andThen(_.tree)

    // ---------------
    // Helper methods
    // ---------------

    /** Does `block` contain nested blocks? */
    private def hasNestedBlocks(block: u.Block): Boolean = {
      lazy val inStats = block.stats.exists {
        case src.ValDef(_, src.Block(_, _), _) => true
        case _ => false
      }
      lazy val inExpr = block.expr match {
        case src.Block(_, _) => true
        case _ => false
      }
      inStats || inExpr
    }

    /** Returns the set of symbols in `tree` that have clashing names. */
    private def nameClashes(tree: u.Tree): Seq[u.TermSymbol] = for {
      (_, defs) <- api.Tree.defs(tree).groupBy(_.name).toSeq
      if defs.size > 1
      dfn <- defs.tail
    } yield dfn

    /** Returns the encoded name associated with this subtree. */
    @tailrec private def nameOf(tree: u.Tree): u.Name = tree match {
      case id: u.Ident => id.name.encodedName
      case value: u.ValDef => value.name.encodedName
      case method: u.DefDef => method.name.encodedName
      case u.Select(_, member) => member.encodedName
      case u.Typed(expr, _) => nameOf(expr)
      case u.Block(_, expr) => nameOf(expr)
      case u.Apply(target, _) => nameOf(target)
      case u.TypeApply(target, _) => nameOf(target)
      case _: u.Function => api.TermName.lambda
      case _ => api.TermName("x")
    }

    /** Decomposes a [[src.Block]] into statements and expressions. */
    private def decompose(tree: u.Tree, simplify: Boolean)
      : (Seq[u.Tree], u.Tree) = tree match {
        case src.Block(stats :+ src.ValDef(x, rhs, _), src.ValRef(y))
          if simplify && x == y => (stats, rhs)
        case src.Block(stats, expr) =>
          (stats, expr)
        case _ =>
          (Seq.empty, tree)
      }

    /** Decomposes a nested sequence [[src.Block]]s into statements and expressions. */
    private def decompose(treess: Seq[Seq[u.Tree]], simplify: Boolean)
      : (Seq[u.Tree], Seq[Seq[u.Tree]]) = {
        val stats = for {
          trees <- treess
          tree <- trees
          stat <- decompose(tree, simplify)._1
        } yield stat

        val exprss = for (trees <- treess)
          yield for (tree <- trees)
            yield decompose(tree, simplify)._2

        (stats, exprss)
      }

    /** Extractor for arbitrary comprehensions. */
    private object Comprehension {
      def unapply(tree: u.Tree): Option[u.Tree] = tree match {
        case src.DefCall(Some(_), method, _, _*)
          if IR.comprehensionOps(method) => Some(tree)
        case _ => None
      }
    }
  }
}

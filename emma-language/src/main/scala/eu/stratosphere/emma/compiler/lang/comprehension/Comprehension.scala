package eu.stratosphere.emma.compiler.lang.comprehension

import eu.stratosphere.emma.compiler.Common
import eu.stratosphere.emma.compiler.lang.core.Core

trait Comprehension extends Common
  with ReDeSugar
  with Normalize {
  self: Core =>

  import universe._
  import Tree._
  import Core.{Lang => core}

  private[emma] object Comprehension {

    // -------------------------------------------------------------------------
    // Mock comprehension syntax language
    // -------------------------------------------------------------------------

    trait MonadOp {
      val symbol: TermSymbol

      def apply(xs: Tree)(fn: Tree): Tree

      def unapply(tree: Tree): Option[(Tree, Tree)]
    }

    class Syntax(val monad: Symbol) {

      //@formatter:off
      val monadTpe  = monad.asType.toType.typeConstructor
      val moduleSel = resolve(IR.module) // API: how to rewrite?
      //@formatter:on

      // -----------------------------------------------------------------------
      // Monad Ops
      // -----------------------------------------------------------------------

      object map extends MonadOp {

        override val symbol =
          api.Term.member(monad, api.TermName("map")).asMethod // API: access method directly

        override def apply(xs: Tree)(f: Tree): Tree =
          core.DefCall(Some(xs))(symbol, elemTpe(f))(f :: Nil)

        override def unapply(apply: Tree): Option[(Tree, Tree)] = apply match {
          case core.DefCall(Some(xs), `symbol`, _, Seq(f)) => Some(xs, f)
          case _ => None
        }

        @inline
        private def elemTpe(f: u.Tree): u.Type =
          api.Type.arg(2, api.Type.of(f))
      }

      object flatMap extends MonadOp {

        override val symbol =
          api.Term.member(monad, api.TermName("flatMap")).asMethod // API: access method directly

        override def apply(xs: u.Tree)(f: u.Tree): u.Tree =
          core.DefCall(Some(xs))(symbol, elemTpe(f))(f :: Nil)

        override def unapply(tree: u.Tree): Option[(u.Tree, u.Tree)] = tree match {
          case core.DefCall(Some(xs), `symbol`, _, Seq(f)) => Some(xs, f)
          case _ => None
        }

        @inline
        private def elemTpe(f: u.Tree): u.Type =
          api.Type.arg(1, api.Type.arg(2, api.Type.of(f)))
      }

      object withFilter extends MonadOp {

        override val symbol =
          api.Term.member(monad, api.TermName("withFilter")).asMethod // API: access method directly

        override def apply(xs: u.Tree)(p: u.Tree): u.Tree =
          core.DefCall(Some(xs))(symbol)(p :: Nil)

        override def unapply(tree: u.Tree): Option[(u.Tree, u.Tree)] = tree match {
          case core.DefCall(Some(xs), `symbol`, _, Seq(p)) => Some(xs, p)
          case _ => None
        }
      }

      // -----------------------------------------------------------------------
      // Mock Comprehension Ops
      // -----------------------------------------------------------------------

      /** Con- and destructs a comprehension from/to a list of qualifiers `qs` and a head expression `hd`. */
      object comprehension {
        val symbol = IR.comprehension

        def apply(qs: Seq[Tree], hd: Tree): Tree =
          core.DefCall(Some(moduleSel))(symbol, elemTpe(hd), monadTpe)(api.Block(qs:_*)(hd) :: Nil)

        def unapply(tree: Tree): Option[(Seq[Tree], Tree)] = tree match {
          case core.DefCall(_, `symbol`, _, api.Block(qs, hd) :: Nil) =>
            Some(qs, hd)
          case _ =>
            None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          api.Type of expr
      }

      /** Con- and destructs a generator from/to a [[Tree]]. */
      object generator {
        val symbol = IR.generator

        def apply(lhs: u.TermSymbol, rhs: u.Block): u.Tree = core.ValDef(
          lhs,
          core.DefCall(Some(moduleSel))(symbol, elemTpe(rhs), monadTpe)(rhs :: Nil))

        def unapply(tree: u.ValDef): Option[(u.TermSymbol, u.Block)] = tree match {
          case core.ValDef(lhs, core.DefCall(_, `symbol`, _, (arg: u.Block) :: Nil), _) =>
            Some(lhs, arg)
          case _ =>
            None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          api.Type.arg(1, api.Type of expr)
      }

      /** Con- and destructs a guard from/to a [[Tree]]. */
      object guard {
        val symbol = IR.guard

        def apply(expr: u.Block): u.Tree =
          core.DefCall(Some(moduleSel))(symbol)(expr :: Nil)

        def unapply(tree: u.Tree): Option[u.Block] = tree match {
          case core.DefCall(_, `symbol`, _, (expr: u.Block) :: Nil) =>
            Some(expr)
          case _ =>
            None
        }
      }

      /** Con- and destructs a head from/to a [[Tree]]. */
      object head {
        val symbol = IR.head

        def apply(expr: u.Block): u.Tree =
          core.DefCall(Some(moduleSel))(symbol, elemTpe(expr))(expr :: Nil)

        def unapply(tree: u.Tree): Option[u.Block] = tree match {
          case core.DefCall(_, `symbol`, _, (expr: u.Block) :: Nil) =>
            Some(expr)
          case _ =>
            None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          api.Type of expr
      }

      /** Con- and destructs a flatten from/to a [[Tree]]. */
      object flatten {
        val symbol = IR.flatten

        def apply(expr: u.Block): u.Tree =
          core.DefCall(Some(moduleSel))(symbol, elemTpe(expr), monadTpe)(expr :: Nil)

        def unapply(tree: u.Tree): Option[u.Block] = tree match {
          case core.DefCall(_, `symbol`, _, (expr: u.Block) :: Nil) =>
            Some(expr)
          case _ =>
            None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          api.Type.arg(1, api.Type.arg(1, api.Type of expr))
      }

    }

    // -------------------------------------------------------------------------
    // ReDeSugar API
    // -------------------------------------------------------------------------

    /** Delegates to [[ReDeSugar.resugar()]]. */
    def resugar(monad: u.Symbol)(tree: u.Tree): u.Tree =
      ReDeSugar.resugar(monad)(tree)

    /** Delegates to [[ReDeSugar.desugar()]]. */
    def desugar(monad: u.Symbol)(tree: u.Tree): u.Tree =
      ReDeSugar.desugar(monad)(tree)

    // -------------------------------------------------------------------------
    // Normalize API
    // -------------------------------------------------------------------------

    /** Delegates to [[Normalize.normalize()]]. */
    def normalize(monad: Symbol)(tree: Tree): Tree =
      Normalize.normalize(monad)(tree)

    // -------------------------------------------------------------------------
    // General helpers
    // -------------------------------------------------------------------------

    def asLet(tree: Tree): Block = tree match {
      case let @ core.Let(_, _, _, _) => let
      case other => core.Let()()()(other)
    }
  }

}

package eu.stratosphere.emma

import eu.stratosphere.emma.api.{DataBag, InputFormat, OutputFormat}
import eu.stratosphere.emma.runtime.AbstractStatefulBackend

import scala.collection.mutable

/**
 * Nodes for building an intermediate representation of an Emma dataflows.
 */
package object ir {

  import scala.language.{existentials, implicitConversions}
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{universe => ru}
  import scala.tools.reflect.ToolBox

  // --------------------------------------------------------------------------
  // Combinators.
  // --------------------------------------------------------------------------

  sealed trait Combinator[+A] {
    implicit val tag: TypeTag[_ <: A]

    /** Apply `f` to each subtree */
    def sequence() = collect({
      case x => x
    })

    /** Apply `pf` to each subexpression on which the function is defined and collect the results. */
    def collect[T](pf: PartialFunction[Combinator[_], T]): List[T] = {
      val ctt = new CollectTreeTraverser[T](pf)
      ctt.traverse(this)
      ctt.results.toList
    }
  }

  final case class Read[+A: TypeTag](location: String, format: InputFormat[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Write[+A: TypeTag](location: String, format: OutputFormat[_ <: A], xs: Combinator[_ <: A]) extends Combinator[Unit] {
    override val tag: TypeTag[Unit] = typeTag[Unit]
  }

  final case class TempSource[+A: TypeTag](ref: DataBag[A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class TempSink[+A: TypeTag](name: String, xs: Combinator[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Map[+A: TypeTag, +B: TypeTag](f: String, xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class FlatMap[+A: TypeTag, +B: TypeTag](f: String, xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Filter[+A: TypeTag](p: String, xs: Combinator[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class EquiJoin[+A: TypeTag, +B: TypeTag, +C: TypeTag](keyx: String, keyy: String, f: String, xs: Combinator[_ <: B], ys: Combinator[_ <: C]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Cross[+A: TypeTag, +B: TypeTag, +C: TypeTag](f: String, xs: Combinator[_ <: B], ys: Combinator[_ <: C]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Group[+A: TypeTag, +B: TypeTag](key: String, xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Fold[+A: TypeTag, +B: TypeTag](empty: String, sng: String, union: String, xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class FoldGroup[+A: TypeTag, +B: TypeTag](key: String, empty: String, sng: String, union: String, xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Distinct[+A: TypeTag](xs: Combinator[A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Union[+A: TypeTag](xs: Combinator[_ <: A], ys: Combinator[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Diff[+A: TypeTag](xs: Combinator[_ <: A], ys: Combinator[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Scatter[+A: TypeTag](xs: Seq[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class StatefulCreate[+S: TypeTag, +K: TypeTag](xs: Combinator[_ <: S]) extends Combinator[Unit] {
    override val tag: TypeTag[Unit] = typeTag[Unit]
    val tagS: TypeTag[_ <: S] = typeTag[S]
    val tagK: TypeTag[_ <: K] = typeTag[K]
  }

  //todo: add variance (+S, +K)
  final case class StatefulFetch[S: TypeTag, K: TypeTag](name: String, stateful: AbstractStatefulBackend[S, K]) extends Combinator[S] {
    override val tag: TypeTag[_ <: S] = typeTag[S]
    val tagK: TypeTag[_ <: K] = typeTag[K]
    val tagAbstractStatefulBackend: TypeTag[AbstractStatefulBackend[S, K]] = typeTag[AbstractStatefulBackend[S, K]]
  }

  final case class UpdateWithZero[S: TypeTag, K: TypeTag, B: TypeTag]
      (name: String, stateful: AbstractStatefulBackend[S, K], udf: String) extends Combinator[B] {
    override val tag: TypeTag[_ <: B] = typeTag[B]
    val tagS: TypeTag[_ <: S] = typeTag[S]
    val tagK: TypeTag[_ <: K] = typeTag[K]
    val tagAbstractStatefulBackend: TypeTag[AbstractStatefulBackend[S, K]] = typeTag[AbstractStatefulBackend[S, K]]
  }

  final case class UpdateWithOne[S: TypeTag, K: TypeTag, A: TypeTag, B: TypeTag]
      (name: String, stateful: AbstractStatefulBackend[S, K], updates: Combinator[_ <: A], updateKeySel: String, udf: String) extends Combinator[B] {
    override val tag: TypeTag[_ <: B] = typeTag[B]
    val tagS: TypeTag[_ <: S] = typeTag[S]
    val tagK: TypeTag[_ <: K] = typeTag[K]
    val tagU: TypeTag[_ <: A] = typeTag[A]
    val tagAbstractStatefulBackend: TypeTag[AbstractStatefulBackend[S, K]] = typeTag[AbstractStatefulBackend[S, K]]
  }

  final case class UpdateWithMany[S: TypeTag, K: TypeTag, A: TypeTag, B: TypeTag]
      (name: String, stateful: AbstractStatefulBackend[S, K], updates: Combinator[_ <: A], updateKeySel: String, udf: String) extends Combinator[B] {
    override val tag: TypeTag[_ <: B] = typeTag[B]
    val tagS: TypeTag[_ <: S] = typeTag[S]
    val tagK: TypeTag[_ <: K] = typeTag[K]
    val tagU: TypeTag[_ <: A] = typeTag[A]
    val tagAbstractStatefulBackend: TypeTag[AbstractStatefulBackend[S, K]] = typeTag[AbstractStatefulBackend[S, K]]
  }

  // --------------------------------------------------------------------------
  // Traversal
  // --------------------------------------------------------------------------

  trait CombinatorTraverser {

    def traverse(e: Combinator[_]): Unit = e match {
      // Combinators
      case Read(_, _)                     =>
      case Write(_, _, xs)                => traverse(xs)
      case TempSource(id)                 =>
      case TempSink(_, xs)                => traverse(xs)
      case Map(_, xs)                     => traverse(xs)
      case FlatMap(_, xs)                 => traverse(xs)
      case Filter(_, xs)                  => traverse(xs)
      case EquiJoin(_, _, _, xs, ys)      => traverse(xs); traverse(ys)
      case Cross(_, xs, ys)               => traverse(xs); traverse(ys)
      case Group(_, xs)                   => traverse(xs)
      case Fold(_, _, _, xs)              => traverse(xs)
      case FoldGroup(_, _, _, _, xs)      => traverse(xs)
      case Distinct(xs)                   => traverse(xs)
      case Union(xs, ys)                  => traverse(xs); traverse(ys)
      case Diff(xs, ys)                   => traverse(xs); traverse(ys)
      case Scatter(_)                     =>
      case StatefulCreate(xs)             => traverse(xs)
      case StatefulFetch(_, _)            =>
      case UpdateWithZero(_, _, _)        =>
      case UpdateWithOne(_, _, us, _, _)  => traverse(us)
      case UpdateWithMany(_, _, us, _, _) => traverse(us)
    }
  }

  private class CollectTreeTraverser[T](pf: PartialFunction[Combinator[_], T]) extends CombinatorTraverser {
    val results = mutable.ListBuffer[T]()

    override def traverse(t: Combinator[_]) {
      super.traverse(t)
      if (pf.isDefinedAt(t)) results += pf(t)
    }
  }

  // --------------------------------------------------------------------------
  // Auxiliary structures
  // --------------------------------------------------------------------------

  def localInputs(e: Combinator[_]): Seq[Seq[_]] = e.collect({
    case Scatter(xs) => xs
  })

  final class UDF(fn: Function, tpe: Type, tb: ToolBox[ru.type]) {

    val tree = fn

    def closure = {
      val vparamsTpes = tpe match {
        case TypeRef(prefix1, sym1, targs1) if UDF.fnSymbols.contains(sym1) => targs1.slice(0, targs1.size - 1)
        case _ => List(tpe)
      }
      for ((vp, tpe) <- tree.vparams zip vparamsTpes) yield ValDef(vp.mods, vp.name, tq"$tpe", vp.rhs)
    }

    def params = {
      val vparamsTpes = tpe match {
        case TypeRef(prefix1, sym1, targs1) if UDF.fnSymbols.contains(sym1) => targs1.reverse.head match {
          case TypeRef(prefix2, sym2, targs2) if UDF.fnSymbols.contains(sym2) => targs2.slice(0, targs2.size - 1)
          case TypeRef(prefix2, sym2, targs2) => targs2
        }
        case _ => List(tpe)
      }
      for ((vp, tpe) <- tree.body.asInstanceOf[Function].vparams zip vparamsTpes) yield ValDef(vp.mods, vp.name, tq"$tpe", vp.rhs)
    }

    def body = tree.body.asInstanceOf[Function].body

    def func = q"(..$params) => $body"
  }

  object UDF {

    val fnSymbols: Set[Symbol] = {
      for (n <- 0 to 22) yield rootMirror.staticClass(s"scala.Function$n")
    }.toSet

    def apply(fn: Tree, tpe: Type, tb: ToolBox[ru.type]) = new UDF(fn.asInstanceOf[Function], tpe, tb)

    def apply(expr: Expr[Any], tb: ToolBox[ru.type]) = new UDF(expr.tree.asInstanceOf[Function], expr.staticType, tb)
  }

  def resultType(tpe: Type): Type = tpe match {
    case TypeRef(prefix, sym, targs) if UDF.fnSymbols.contains(sym) => resultType(targs.reverse.head)
    case _ => tpe
  }
}

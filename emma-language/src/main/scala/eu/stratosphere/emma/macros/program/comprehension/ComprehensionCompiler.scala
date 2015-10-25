package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionCombination
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel

private[emma] trait ComprehensionCompiler
    extends ControlFlowModel
    with ComprehensionAnalysis
    with ComprehensionCombination {

  import universe._

  /**
   * Compile a generic driver for a data-parallel runtime.
   *
   * @param tree The original program [[Tree]]
   * @param cfGraph The control flow graph representation of the [[Tree]]
   * @param compView A view over the comprehended terms in the [[Tree]]
   * @return A [[Tree]] representing the compiled driver
   */
  def compile(tree: Tree, cfGraph: CFGraph, compView: ComprehensionView): Tree =
    new Compiler(cfGraph, compView).transform(tree).unTypeChecked

  private class Compiler(cfGraph: CFGraph, compView: ComprehensionView) extends Transformer {

    override def transform(tree: Tree): Tree = compView getByTerm tree match {
      case Some(term) => expandComprehension(tree, cfGraph, compView)(term)
      case None       => super.transform(tree)
    }

    /**
     * Expand a comprehended term in the compiled driver.
     *
     * @param tree The original program [[Tree]]
     * @param cfGraph The control flow graph representation of the [[Tree]]
     * @param compView A view over the comprehended terms in the [[Tree]]
     * @param term The comprehended term to be expanded
     * @return A [[Tree]] representing the expanded comprehension
     */
    private def expandComprehension(tree: Tree, cfGraph: CFGraph, compView: ComprehensionView)
        (term: ComprehendedTerm) = {

      // Apply combinators to get a purely-functional, logical plan
      val root = combine(term.comprehension).expr

      // Extract the comprehension closure
      val closure = term.comprehension.freeTerms

      // Get the TermName associated with this comprehended term
      val name = term.id.encodedName

      val execute = root match {
        case _: combinator.Write          => TermName("executeWrite")
        case _: combinator.Fold           => TermName("executeFold")
        case _: combinator.StatefulCreate => TermName("executeStatefulCreate")
        case _: combinator.UpdateWithZero => TermName("updateWithZero")
        case _: combinator.UpdateWithOne  => TermName("updateWithOne")
        case _: combinator.UpdateWithMany => TermName("updateWithMany")
        case _                            => TermName("executeTempSink")
      }

      q"""{
        import _root_.scala.reflect.runtime.universe._
        import _root_.eu.stratosphere.emma.ir
        val __root   = ${serialize(root)}
        val __result = engine.$execute(__root, ${name.toString}, ..${
          for (s <- closure) yield if (s.isMethod) q"${s.name} _" else q"${s.name}"
        })
        __result
      }"""
    }

    /**
     * Serializes a macro-level IR [[Tree]] as code constructing an equivalent runtime-level IR
     * [[Tree]].
     *
     * @param expr The [[Expression]] in IR to be serialized
     * @return A [[String]] representation of the [[Expression]]
     */
    private def serialize(expr: Expression): Tree = expr match {
      case combinator.Read(loc, fmt) =>
        q"ir.Read($loc, $fmt)"

      case combinator.Write(loc, fmt, xs) =>
        q"ir.Write($loc, $fmt, ${serialize(xs)})"

      case combinator.TempSource(id) =>
        q"ir.TempSource($id)"

      case combinator.TempSink(name, xs) =>
        q"ir.TempSink(${name.toString}, ${serialize(xs)})"

      case combinator.Map(f, xs) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val fStr  = serialize(f)
        val xsStr = serialize(xs)
        q"ir.Map[$elTpe, $xsTpe]($fStr, $xsStr)"

      case combinator.FlatMap(f, xs) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val fStr  = serialize(f)
        val xsStr = serialize(xs)
        q"ir.FlatMap[$elTpe, $xsTpe]($fStr, $xsStr)"

      case combinator.Filter(p, xs) =>
        val elTpe = expr.elementType
        val pStr  = serialize(p)
        val xsStr = serialize(xs)
        q"ir.Filter[$elTpe]($pStr, $xsStr)"

      case combinator.EquiJoin(kx, ky, xs, ys) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val ysTpe = ys.elementType
        val kxStr = serialize(kx)
        val kyStr = serialize(ky)
        val xsStr = serialize(xs)
        val ysStr = serialize(ys)
        val join  = serialize(q"(x: $xsTpe, y: $ysTpe) => (x, y)".typeChecked)
        q"ir.EquiJoin[$elTpe, $xsTpe, $ysTpe]($kxStr, $kyStr, $join, $xsStr, $ysStr)"

      case combinator.Cross(xs, ys) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val ysTpe = ys.elementType
        val xsStr = serialize(xs)
        val ysStr = serialize(ys)
        val join  = serialize(q"(x: $xsTpe, y: $ysTpe) => (x, y)".typeChecked)
        q"ir.Cross[$elTpe, $xsTpe, $ysTpe]($join, $xsStr, $ysStr)"

      case combinator.Group(key, xs) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val kStr  = serialize(key)
        val xsStr = serialize(xs)
        q"ir.Group[$elTpe, $xsTpe]($kStr, $xsStr)"

      case combinator.Fold(empty, sng, union, xs, _) =>
        val exprTpe  = expr.tpe
        val xsTpe    = xs.elementType
        val emptyStr = serialize(empty)
        val sngStr   = serialize(sng)
        val unionStr = serialize(union)
        val xsStr    = serialize(xs)
        q"ir.Fold[$exprTpe, $xsTpe]($emptyStr, $sngStr, $unionStr, $xsStr)"

      case combinator.FoldGroup(key, empty, sng, union, xs) =>
        val elTpe    = expr.elementType
        val xsTpe    = xs.elementType
        val keyStr   = serialize(key)
        val emptyStr = serialize(empty)
        val sngStr   = serialize(sng)
        val unionStr = serialize(union)
        val xsStr    = serialize(xs)
        q"ir.FoldGroup[$elTpe, $xsTpe]($keyStr, $emptyStr, $sngStr, $unionStr, $xsStr)"

      case combinator.Distinct(xs) =>
        q"ir.Distinct(${serialize(xs)})"

      case combinator.Union(xs, ys) =>
        q"ir.Union(${serialize(xs)}, ${serialize(ys)})"

      case combinator.Diff(xs, ys) =>
        q"ir.Diff(${serialize(xs)}, ${serialize(ys)})"

      case ScalaExpr(Apply(fn, values :: Nil))
        if api.apply.alternatives contains fn.symbol =>
          q"ir.Scatter(${transform(values)})"

      case combinator.StatefulCreate(xs, stateType, keyType) =>
        val xsStr = serialize(xs)
        q"ir.StatefulCreate[$stateType, $keyType]($xsStr)"

      case combinator.StatefulFetch(stateful) =>
        val statefulNameStr = stateful.name.toString
        q"ir.StatefulFetch($statefulNameStr, $stateful)"

      case combinator.UpdateWithZero(stateful, udf) =>
        val stateTpe = stateful.trueType.typeArgs.head
        val keyTpe   = stateful.trueType.typeArgs(1)
        val outTpe   = expr.elementType
        val statefulNameStr = stateful.name.toString
        val udfStr          = serialize(udf)
        q"ir.UpdateWithZero[$stateTpe, $keyTpe, $outTpe]($statefulNameStr, $stateful, $udfStr)"

      case combinator.UpdateWithOne(stateful, upds, keySel, udf) =>
        val stateTpe = stateful.trueType.typeArgs.head
        val keyTpe   = stateful.trueType.typeArgs(1)
        val updTpe   = upds.elementType
        val outTpe   = expr.elementType
        val statefulNameStr = stateful.name.toString
        val usStr           = serialize(upds)
        val keySelStr       = serialize(keySel)
        val udfStr          = serialize(udf)
        q"ir.UpdateWithOne[$stateTpe, $keyTpe, $updTpe, $outTpe]($statefulNameStr, $stateful, $usStr, $keySelStr, $udfStr)"

      case combinator.UpdateWithMany(stateful, upds, keySel, udf) =>
        val stateTpe = stateful.trueType.typeArgs.head
        val keyTpe   = stateful.trueType.typeArgs(1)
        val updTpe   = upds.elementType
        val outTpe   = expr.elementType
        val statefulNameStr = stateful.name.toString
        val usStr           = serialize(upds)
        val keySelStr       = serialize(keySel)
        val udfStr          = serialize(udf)
        q"ir.UpdateWithMany[$stateTpe, $keyTpe, $updTpe, $outTpe]($statefulNameStr, $stateful, $usStr, $keySelStr, $udfStr)"

      case e => EmptyTree
      //throw new RuntimeException(
      //  s"Unsupported serialization of non-combinator expression:\n${prettyPrint(e)}\n")
    }

    /**
     * Emits a reified version of the given term [[Tree]].
     *
     * @param tree The [[Tree]] to be serialized
     * @return A [[String]] representation of the [[Tree]]
     */
    private def serialize(tree: Tree): String = {
      val args = tree.freeTerms.toList
        .sortBy { _.fullName }
        .map { sym =>
          sym.name ->
            (sym.info match {
              // The following line converts a method type to a function type (eg. from "(x: Int)Int" to "Int => Int").
              // This is necessary, because this will go into a parameter list, where we can't have method types.
              case _: MethodType => q"$sym _".trueType
              case _             => sym.info
            })
        }

      val aaa = showCode(tree)
      var bbb = ""
      var ccc: Any = null
      var ddd: Any = null
      try {
//        bbb = showCode(tree.asInstanceOf[Function].body.asInstanceOf[Select].qualifier.asInstanceOf[Block].
//          stats.apply(0).asInstanceOf[ValDef].rhs.asInstanceOf[Match].selector.asInstanceOf[Typed].expr.asInstanceOf[Select].qualifier)

        ccc = tree.asInstanceOf[Function].body.asInstanceOf[Select].qualifier.asInstanceOf[Block].
          stats.apply(0).asInstanceOf[ValDef].rhs.asInstanceOf[Match].selector

        bbb = showCode(ccc.asInstanceOf[Typed])

        ddd = showCode(c.untypecheck(ccc.asInstanceOf[Tree]))

//        bbb = showCode(tree.asInstanceOf[Function].body.asInstanceOf[Select].qualifier.asInstanceOf[Block].
//          stats.apply(0).asInstanceOf[ValDef].rhs.asInstanceOf[Match].selector)
      } catch {
        case ex: Throwable =>

      }

      val fun = mk.anonFun(args, tree)
      val foo = showCode(fun, printRootPkg = true)
      foo
    }
  }
}

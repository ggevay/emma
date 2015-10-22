package eu.stratosphere.emma.macros.program.comprehension.rewrite

trait ComprehensionCombination extends ComprehensionRewriteEngine {
  import universe._

  def combine(root: ExpressionRoot) = {
    // states for the state machine
    sealed trait RewriteState
    object Start  extends RewriteState
    object Filter extends RewriteState
    object Join   extends RewriteState
    object Cross  extends RewriteState
    object Map    extends RewriteState
    object End    extends RewriteState

    // state machine for the rewrite process
    def process(state: RewriteState): ExpressionRoot = state match {
      case Start  => process(Filter);
      case Filter => applyExhaustively(MatchFilter)(root); process(Join)
      case Join   => if (applyAny(MatchEquiJoin)(root)) process(Filter) else process(Cross)
      case Cross  => if (applyAny(MatchCross)(root))    process(Filter) else process(Map)
      case Map    => applyExhaustively(MatchMap, MatchFlatMap)(root); process(End)
      case End    => root
    }

    // (1) run the state machine and (2) simplify UDF parameter names
    process(Start)
  }

  //---------------------------------------------------------------------------
  // MAP, FLATMAP, FILTER
  //---------------------------------------------------------------------------

  /**
   * Creates a filter combinator.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ e | qs, x ← xs, qs1, p x, qs2 ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e | qs, x ← filter p xs, qs1, qs2 ]] }}}
   */
  object MatchFilter extends Rule {

    case class RuleMatch(root: Expression, parent: Comprehension, gen: Generator, filter: Filter)

    def bind(expr: Expression, root: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case parent @ Comprehension(_, qualifiers) => for {
          filter @ Filter(_)    <- qualifiers
          gen @ Generator(_, _) <- qualifiers takeWhile { _ != filter }
        } f(RuleMatch(root, parent, gen, filter))
          
        case _ =>
      }
    }

    /**
     * Checks:
     *
     * - Filter uses only 1 variable
     * - Generator binds the variable of the filter
     */
    // TODO: add support for comprehension filter expressions
    def guard(rm: RuleMatch) = rm.filter.expr match {
      case expr: ScalaExpr =>
        expr.usedVars(rm.root) subsetOf Set(rm.gen.lhs)
        
      case _ => false
    }

    // TODO: add support for comprehension filter expressions
    def fire(rm: RuleMatch) = rm.filter.expr match {
      case expr @ ScalaExpr(tree) =>
        val RuleMatch(root, parent, gen, filter) = rm
        val p    = mk anonFun (List(rm.gen.lhs.name -> rm.gen.lhs.info), tree)
        gen.rhs           = combinator.Filter(p, gen.rhs)
        parent.qualifiers = parent.qualifiers diff List(filter)
        parent // return new parent

      case filter => c.abort(c.enclosingPosition,
        s"Unexpected filter expression type: ${filter.getClass}")
    }
  }

  /**
   * Creates a map combinator. Assumes that aggregations are matched beforehand.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ f x  | x ← xs ]] }}}
   *
   * '''Rewrite''':
   * {{{ map f xs }}}
   */
  object MatchMap extends Rule {

    case class RuleMatch(root: Expression, head: ScalaExpr, child: Generator)

    def bind(expr: Expression, root: Expression) = expr match {
      case Comprehension(head: ScalaExpr, List(child: Generator)) =>
        Some(RuleMatch(root, head, child))
      
      case _ => None
    }

    def guard(rm: RuleMatch) = {
      rm.head.usedVars(rm.root) subsetOf Set(rm.child.lhs)
    }

    def fire(rm: RuleMatch) = {
      val RuleMatch(root, expr @ ScalaExpr(tree), child) = rm
      val f = mk anonFun (List(child.lhs.name -> child.lhs.info), tree)
      combinator.Map(f, child.rhs)
    }
  }

  /**
   * Creates a flatMap combinator. Assumes that aggregations are matched beforehand.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ join [[ f x  | x ← xs ]] }}}
   *
   * '''Rewrite''':
   * {{{ flatMap f xs }}}
   */
  object MatchFlatMap extends Rule {

    case class RuleMatch(root: Expression, head: ScalaExpr, child: Generator)

    def bind(expr: Expression, root: Expression) = expr match {
      case MonadJoin(Comprehension(head: ScalaExpr, List(child: Generator))) =>
        Some(RuleMatch(root, head, child))
      
      case _ => None
    }

    def guard(rm: RuleMatch) = {
      rm.head.usedVars(rm.root) subsetOf Set(rm.child.lhs)
    }

    def fire(rm: RuleMatch) = {
      val RuleMatch(root, expr @ ScalaExpr(tree), child) = rm
      val f = mk anonFun (List(child.lhs.name -> child.lhs.info), tree)
      combinator.FlatMap(f, child.rhs)
    }
  }

  //---------------------------------------------------------------------------
  // JOINS
  //---------------------------------------------------------------------------

  /**
   * Creates an equi-join combinator.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ e | qs, x ← xs, y ← ys, qs1, k₁ x == k₂ y, qs2 ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e[v.x/x][v.y/y] | qs, v ← ⋈ k₁ k₂ xs ys, qs1[v.x/x][v.y/y], qs2[v.x/x][v.y/y] ]] }}}
   */
  object MatchEquiJoin extends Rule {

    case class RuleMatch(
      root:   Expression,
      parent: Comprehension,
      xs:     Generator,
      ys:     Generator,
      filter: Filter,
      kx:     Function,
      ky:     Function)

    def bind(expr: Expression, root: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case parent @ Comprehension(_, qualifiers) => for {
          filter @ Filter(p: ScalaExpr) <- qualifiers
          pairs = qualifiers takeWhile { _ != filter } sliding 2
          (xs: Generator) :: (ys: Generator) :: Nil <- pairs
          (kx, ky) <- parseJoinPredicate(root, xs, ys, p)
        } f(RuleMatch(root, parent, xs, ys, filter, kx, ky))

        case _ =>
      }
    }

    def guard(rm: RuleMatch) = true

    def fire(rm: RuleMatch) = {
      val RuleMatch(root, parent, xs, ys, filter, kx, ky) = rm
      val (prefix, suffix) = parent.qualifiers span { _ != xs }
      // construct combinator node with input and predicate sides aligned
      val join = combinator.EquiJoin(kx, ky, xs.rhs, ys.rhs)

      // bind join result to a fresh variable
      val tpt = tq"(${kx.vparams.head.tpt}, ${ky.vparams.head.tpt})"
      val vd  = mk.valDef(freshName("x"), tpt)
      val sym = mk.freeTerm(vd.name.toString, vd.trueType)
      val qs  = suffix drop 2 filter { _ != filter }

      // substitute [v._1/x] in affected expressions
      for {
        expr @ ScalaExpr(_) <- parent
        if expr.usedVars(root) contains { xs.lhs }
        tree = q"${mk ref sym}._1" withType vd.trueType.typeArgs(0)
      } expr.substitute(xs.lhs.name, ScalaExpr(tree))

      // substitute [v._2/y] in affected expressions
      for {
        expr @ ScalaExpr(_) <- parent
        if expr.usedVars(root) contains { ys.lhs }
        tree = q"${mk ref sym}._2" withType vd.trueType.typeArgs(1)
      } expr.substitute(ys.lhs.name, ScalaExpr(tree))

      // modify parent qualifier list
      parent.qualifiers = prefix ::: Generator(sym, join) :: qs

      // return the modified parent
      parent
    }

    private def parseJoinPredicate(root: Expression, xs: Generator, ys: Generator, p: ScalaExpr):
      Option[(Function, Function)] = p.tree match {
        case q"${lhs: Tree} == ${rhs: Tree}" =>
          val defVars = p.definedVars(root)

          val lhsVars = defVars intersect lhs.references
          val rhsVars = defVars intersect rhs.references

          if (lhsVars.size != 1 || rhsVars.size != 1) {
            None // Both `lhs` and `rhs` must refer to exactly one variable
          } else if ((lhsVars contains xs.lhs) &&
                     (rhsVars contains ys.lhs)) {
            // Filter expression has the type `f(xs.lhs) == h(ys.lhs)`
            val vx = xs.lhs.name -> xs.lhs.info
            val vy = ys.lhs.name -> ys.lhs.info
            val kx = mk anonFun (List(vx), lhs)
            val ky = mk anonFun (List(vy), rhs)
            Some(kx, ky)
          } else if ((lhsVars contains ys.lhs) &&
                     (rhsVars contains xs.lhs)) {
            // Filter expression has the type `f(ys.lhs) == h(xs.lhs)`
            val vx = xs.lhs.name -> xs.lhs.info
            val vy = ys.lhs.name -> ys.lhs.info
            val kx = mk anonFun (List(vx), rhs)
            val ky = mk anonFun (List(vy), lhs)
            Some(kx, ky)
          } else None  // Something else

        case _ => None // Something else
      }
  }

  //----------------------------------------------------------------------------
  // CROSS
  //----------------------------------------------------------------------------

  /**
   * Creates a cross combinator.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ e | qs, x ← xs, y ← ys, qs1 ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e[v.x/x][v.y/y] | qs, v ← ⨯ xs ys, qs1[v.x/x][v.y/y] ]] }}}
   */
  object MatchCross extends Rule {

    case class RuleMatch(root: Expression, parent: Comprehension, xs: Generator, ys: Generator)

    def bind(expr: Expression, root: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case parent @ Comprehension(_, qs) =>
          for ((xs: Generator) :: (ys: Generator) :: Nil <- qs sliding 2)
            f(RuleMatch(root, parent, xs, ys))

        case _ =>
      }
    }

    def guard(rm: RuleMatch) = true

    def fire(rm: RuleMatch) = {
      val RuleMatch(root, parent, xs, ys) = rm
      val (prefix, suffix) = parent.qualifiers span { _ != xs }

      // construct combinator node with input and predicate sides aligned
      val cross = combinator.Cross(xs.rhs, ys.rhs)

      // bind join result to a fresh variable
      val vd  = mk.valDef(freshName("x"), tq"(${rm.xs.tpe}, ${rm.ys.tpe})")
      val sym = mk.freeTerm(vd.name.toString, vd.trueType)

      // substitute [v._1/x] in affected expressions
      for {
        expr @ ScalaExpr(_) <- parent
        if expr.usedVars(root) contains { xs.lhs }
        tree = q"${mk ref sym}._1" withType vd.trueType.typeArgs(0)
      } expr.substitute(xs.lhs.name, ScalaExpr(tree))

      // substitute [v._2/y] in affected expressions
      for {
        expr @ ScalaExpr(_) <- parent
        if expr.usedVars(root) contains { ys.lhs }
        tree = q"${mk ref sym}._2" withType vd.trueType.typeArgs(1)
      } expr.substitute(ys.lhs.name, ScalaExpr(tree))

      // modify parent qualifier list
      parent.qualifiers = prefix ::: Generator(sym, cross) :: suffix.drop(2)

      // return the modified parent
      rm.parent
    }
  }
}


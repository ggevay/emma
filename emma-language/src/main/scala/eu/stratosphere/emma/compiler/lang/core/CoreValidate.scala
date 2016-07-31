package eu.stratosphere.emma
package compiler.lang.core

import compiler.Common

/** Validation for the [[Core]] language. */
private[core] trait CoreValidate extends Common {
  self: Core =>

  import universe._
  import Validation._
  import Core.{Lang => core}

  /** Validation for the [[Core]] language. */
  private[core] object CoreValidate {

    /** Fluid [[Validator]] builder. */
    implicit private class Check(tree: u.Tree) {

      /** Provide [[Validator]] to test. */
      case class is(expected: Validator) {

        /** Provide error message in case of validation failure. */
        def otherwise(violation: => String): Verdict =
        validateAs(expected, tree, violation)
      }
    }

    /** Validators for the [[Core]] language. */
    object valid {

      /** Validates that a Scala AST belongs to the supported [[Core]] language. */
      // TODO: Narrow scope of valid top-level trees
      def apply(tree: Tree): Verdict =
        tree is oneOf(Term, Let) otherwise "Unexpected tree"

      // ---------------------------------------------------------------------------
      // Atomics
      // ---------------------------------------------------------------------------

      lazy val Lit: Validator = {
        case core.Lit(_) => pass
      }

      lazy val Ref: Validator =
        oneOf(BindingRef, ModuleRef)

      lazy val This: Validator = {
        case core.This(_) => pass
      }

      lazy val Atomic: Validator =
        oneOf(Lit, Ref, This)

      // ---------------------------------------------------------------------------
      // Parameters
      // ---------------------------------------------------------------------------

      lazy val ParRef: Validator = {
        case core.ParRef(_) => pass
      }

      lazy val ParDef: Validator = {
        case core.ParDef(_, api.Tree.empty, _) => pass
      }

      // ---------------------------------------------------------------------------
      // Values
      // ---------------------------------------------------------------------------

      lazy val ValRef: Validator = {
        case core.ValRef(_) => pass
      }

      lazy val ValDef: Validator = {
        case core.ValDef(_, rhs, _) =>
          rhs is Term otherwise s"Invalid ${core.ValDef} RHS"
      }

      // ---------------------------------------------------------------------------
      // Bindings
      // ---------------------------------------------------------------------------

      lazy val BindingRef: Validator =
        oneOf(ValRef, ParRef)

      lazy val BindingDef: Validator =
        oneOf(ValDef, ParDef)

      // ---------------------------------------------------------------------------
      // Modules
      // ---------------------------------------------------------------------------

      lazy val ModuleRef: Validator = {
        case core.ModuleRef(_) => pass
      }

      lazy val ModuleAcc: Validator = {
        case core.ModuleAcc(target, _) =>
          target is Atomic otherwise s"Invalid ${core.ModuleAcc} target"
      }

      // ---------------------------------------------------------------------------
      // Methods
      // ---------------------------------------------------------------------------

      lazy val DefCall: Validator = {
        case core.DefCall(None, _, _, argss@_*) =>
          all (argss.flatten) are Atomic otherwise s"Invalid ${core.DefCall} argument"
        case core.DefCall(Some(target), _, _, argss@_*) => {
          target is Atomic otherwise s"Invalid ${core.DefCall} target"
        } and {
          all (argss.flatten) are Atomic otherwise s"Invalid ${core.DefCall} argument"
        }
      }

      lazy val DefDef: Validator = {
        case core.DefDef(_, _, _, paramss, body) => {
          all (paramss.flatten) are ParDef otherwise s"Invalid ${core.DefDef} parameter"
        } and {
          body is Let otherwise s"Invalid ${core.DefDef} body"
        }
      }

      // ---------------------------------------------------------------------------
      // Definitions
      // ---------------------------------------------------------------------------

      lazy val TermDef: Validator =
        oneOf(BindingDef, DefDef)

      // ---------------------------------------------------------------------------
      // Terms
      // ---------------------------------------------------------------------------

      lazy val Branch: Validator = {
        case core.Branch(cond, thn, els) => {
          cond is Atomic otherwise s"Invalid ${core.Branch} condition"
        } and {
          all (thn, els) are oneOf(Atomic, DefCall) otherwise s"Invalid ${core.Branch} expression"
        }
      }

      lazy val Inst: Validator = {
        case core.Inst(_, _, argss@_*) =>
          all (argss.flatten) are Atomic otherwise s"Invalid ${core.Inst} argument"
      }

      lazy val Lambda: Validator = {
        case core.Lambda(_, params, body) => {
          all (params) are ParDef otherwise s"Invalid ${core.Lambda} parameter"
        } and {
          body is Let otherwise s"Invalid ${core.Lambda} body"
        }
      }

      lazy val TypeAscr: Validator = {
        case core.TypeAscr(expr, _) =>
          expr is Atomic otherwise s"Invalid ${core.TypeAscr} expression"
      }

      lazy val Term: Validator =
        oneOf(Atomic, ModuleAcc, Inst, DefCall, Branch, Lambda, TypeAscr)

      // ---------------------------------------------------------------------------
      // Let-in blocks
      // ---------------------------------------------------------------------------

      lazy val Let: Validator = {
        case core.Let(vals, defs, expr) => {
          all (vals) are ValDef otherwise s"Invalid ${core.Let} binding"
        } and {
          all (defs) are DefDef otherwise s"Invalid ${core.Let} function"
        } and {
          expr is oneOf(Atomic, DefCall, Branch) otherwise s"Invalid ${core.Let} expression"
        }
      }
    }
  }
}

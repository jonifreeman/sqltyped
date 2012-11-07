package sqltyped

import scala.reflect.runtime.universe.{Type, typeOf}
import Ast.Resolved._

class TypeSigDSL(typer: Typer) {
  case class f[A: Typed](a: A) {
    def ->[R: Typed](r: R) = (fname: String, params: List[Expr]) =>
      if (params.length != 1) fail("Expected 1 parameter " + params)
      else for {
        a1 <- implicitly[Typed[A]].tpe(fname, params(0))
        r  <- implicitly[Typed[R]].tpe(fname, params.head) // FIXME params.head ??
      } yield (List(a1), r)
  }

  case class f2[A: Typed, B: Typed](a: A, b: B) {
    def ->[R: Typed](r: R) = (fname: String, params: List[Expr]) =>
      if (params.length != 2) fail("Expected 2 parameters " + params)
      else for {
        a1 <- implicitly[Typed[A]].tpe(fname, params(0))
        a2 <- implicitly[Typed[B]].tpe(fname, params(1))
        r  <- implicitly[Typed[R]].tpe(fname, params.head) // FIXME params.head ??
      } yield (List(a1, a2), r)
  }

  trait Typed[A] {
    def tpe(fname: String, e: Expr): ?[(Type, Boolean)]
  }
  
  trait TypeParam
  object a extends TypeParam
  object b extends TypeParam
  object c extends TypeParam
  object d extends TypeParam

  object int
  object long
  object double
  object date
  case class option[A: Typed](a: A)

  implicit def optionTyped[A: Typed]: Typed[option[A]] = new Typed[option[A]] {
    def tpe(fname: String, e: Expr) = implicitly[Typed[A]].tpe(fname, e) map { 
      case (tpe, opt) => (tpe, true) 
    }
  }

  implicit def typeParamTyped[A <: TypeParam]: Typed[A] = new Typed[A] {
    def tpe(fname: String, e: Expr) = typer.tpeOf(e) map { 
      case (tpe, opt) => (tpe, typer.isAggregate(fname) || opt) 
    }
  }

  implicit def intTyped: Typed[int.type] = new Const[int.type](typeOf[Int])
  implicit def longTyped: Typed[long.type] = new Const[long.type](typeOf[Long])
  implicit def doubleTyped: Typed[double.type] = new Const[double.type](typeOf[Double])
  implicit def dateTyped: Typed[date.type] = new Const[date.type](typeOf[java.util.Date])

  class Const[A](tpe: Type) extends Typed[A] {
    def tpe(fname: String, e: Expr) = (tpe, false).ok
  }
}

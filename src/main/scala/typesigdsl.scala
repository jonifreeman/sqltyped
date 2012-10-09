package sqltyped

import scala.reflect.runtime.universe.{Type, typeOf}

class TypeSigDSL(typer: Typer) extends Ast.Resolved {
  case class f[A: Typed](a: A) {
    def ->[B: Typed](b: B) = (fname: String, params: List[Term]) =>
      if (params.length != 1) fail("Expected 1 parameter " + params)
      else implicitly[Typed[B]].tpe(fname, params.head) 
  }

  trait Typed[A] {
    def tpe(fname: String, e: Term): ?[(Type, Boolean)]
  }
  
  trait TypeParam
  object a extends TypeParam
  object b extends TypeParam
  object c extends TypeParam
  object d extends TypeParam

  object int
  object long
  object double
  case class option[A: Typed](a: A)

  implicit def optionTyped[A: Typed]: Typed[option[A]] = new Typed[option[A]] {
    def tpe(fname: String, e: Term) = implicitly[Typed[A]].tpe(fname, e) map { 
      case (tpe, opt) => (tpe, true) 
    }
  }

  implicit def typeParamTyped[A <: TypeParam]: Typed[A] = new Typed[A] {
    def tpe(fname: String, e: Term) = typer.tpeOf(e) map { 
      case (tpe, opt) => (tpe, typer.isAggregate(fname) || opt) 
    }
  }

  implicit def intTyped: Typed[int.type] = new Const[int.type](typeOf[Int])
  implicit def longTyped: Typed[long.type] = new Const[long.type](typeOf[Long])
  implicit def doubleTyped: Typed[double.type] = new Const[double.type](typeOf[Double])

  class Const[A](tpe: Type) extends Typed[A] {
    def tpe(fname: String, e: Term) = (tpe, false).ok
  }
}

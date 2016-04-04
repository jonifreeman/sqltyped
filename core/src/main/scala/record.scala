package sqltyped

import shapeless._, ops.hlist._, ops.record._, labelled.FieldType

object Record {
  private object fieldToUntyped extends Poly1 {
    implicit def f[F, V](implicit wk: shapeless.Witness.Aux[F]) = at[FieldType[F, V]] { 
      f => (wk.value.toString, f: Any) :: Nil
    }
  }

  def toTupleLists[R <: HList, F, V](rs: List[R])
    (implicit folder: MapFolder[R, List[(String, Any)], fieldToUntyped.type]): List[List[(String, Any)]] = 
      rs map (r => toTupleList(r)(folder))

  def toTupleList[R <: HList, F, V](r: R)
    (implicit folder: MapFolder[R, List[(String, Any)], fieldToUntyped.type]): List[(String, Any)] =
      r.foldMap(Nil: List[(String, Any)])(fieldToUntyped)(_ ::: _)
}

private[sqltyped] object showField extends Poly1 {
  implicit def f[F, V](implicit wk: shapeless.Witness.Aux[F]) = at[FieldType[F, V]] {
    f => wk.value.toString + " = " + f.toString
  }
}

final class RecordOps[R <: HList](r: R) {

  def show(implicit folder: ops.hlist.MapFolder[R, String, showField.type]): String = {
    val concat = (s1: String, s2: String) => if (s2 != "") s1 + ", " + s2 else s1
    "{ " + r.foldMap("")(showField)(concat) + " }"
  }
}

final class ListOps[L <: HList](l: List[L]) {
  def values(implicit values: Values[L]): List[values.Out] = l map (r => values(r))

  def tuples[Out0 <: HList, Out <: Product]
    (implicit 
       values: Values.Aux[L, Out0],
       tupler: Tupler.Aux[Out0, Out]): List[Out] = l map (r => tupler(values(r)))
}

final class OptionOps[L <: HList](o: Option[L]) {
  def values(implicit values: Values[L]): Option[values.Out] = o map (r => values(r))

  def tuples[Out0 <: HList, Out <: Product]
    (implicit 
       values: Values.Aux[L, Out0],
       tupler: Tupler.Aux[Out0, Out]): Option[Out] = o map (r => tupler(values(r)))
}

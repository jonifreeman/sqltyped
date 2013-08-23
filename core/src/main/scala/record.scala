package sqltyped

import shapeless._, ops.hlist._, ops.record._

object Record {
/*
  def toTupleLists[R <: HList](rs: List[R])(implicit toList: ToList[R, (Any, Any)]): List[List[(String, Any)]] = rs map (r => toTupleList(r)(toList))

  def toTupleList[R <: HList](r: R)(implicit toList: ToList[R, (Any, Any)]): List[(String, Any)] = 
    r.toList map { case (k, v) => (keyAsString(k), v) }
    */
}

final class ListOps[L <: HList](l: List[L]) {
  def values(implicit values: Values[L]): List[values.Out] = l map (r => values(r))

  def tuples[Out0 <: HList, Out <: Product]
    (implicit 
       values: Values.Aux[L, Out0],
       tupler: Tupler.Aux[Out0, Out]) = l map (r => tupler(values(r)))
}

final class OptionOps[L <: HList](o: Option[L]) {
  def values(implicit values: Values[L]): Option[values.Out] = o map (r => values(r))

  def tuples[Out0 <: HList, Out <: Product]
    (implicit 
       values: Values.Aux[L, Out0],
       tupler: Tupler.Aux[Out0, Out]) = o map (r => tupler(values(r)))
}

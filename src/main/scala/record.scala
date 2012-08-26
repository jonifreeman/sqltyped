package sqltyped

import shapeless._

final class AssocHListOps[L <: HList](l: L) {
  def lookup[K](implicit lookup: Lookup[L, K]): lookup.Out = lookup(l)

  def get[K](k: K)(implicit lookup0: Lookup[L, K]): lookup0.Out = lookup[K]

  def values(implicit valueProj: ValueProjection[L]): valueProj.Out = valueProj(l)
  def values0[Out <: HList](implicit valueProj: ValueProjectionAux[L, Out]): Out = valueProj(l)
}

final class ListOps[L <: HList](l: List[L]) {
  def values(implicit valueProj: ValueProjection[L]): List[valueProj.Out] = l.map(_.values)

  def tuples[Out0 <: HList, Out <: Product]
    (implicit 
       valueProj: ValueProjectionAux[L, Out0],
       tupler: TuplerAux[Out0, Out]) = l.map(_.values0.tupled)
}

final class OptionOps[L <: HList](o: Option[L]) {
  def values(implicit valueProj: ValueProjection[L]): Option[valueProj.Out] = o.map(_.values)

  def tuples[Out0 <: HList, Out <: Product]
    (implicit 
       valueProj: ValueProjectionAux[L, Out0],
       tupler: TuplerAux[Out0, Out]) = o.map(_.values0.tupled)
}

@annotation.implicitNotFound(msg = "No such column ${K}")
trait Lookup[L <: HList, K] {
  type Out
  def apply(l: L): Out
}

object Lookup {
  implicit def hlistLookup1[K, V, T <: HList] = new Lookup[(K, V) :: T, K] {
    type Out = V
    def apply(l: (K, V) :: T) = l.head._2
  }

  implicit def hlistLookup[K, V, T <: HList, K1, V1](implicit st: Lookup[T, K1]) = new Lookup[(K, V) :: T, K1] {
    type Out = st.Out
    def apply(l: (K, V) :: T) = st(l.tail)
  }
}

trait ValueProjection[L <: HList] {
  type Out <: HList
  def apply(l: L): Out
}

trait ValueProjectionAux[L <: HList, Out <: HList] {
  def apply(l: L): Out
}

object ValueProjection {
  implicit def valueProjection[L <: HList, Out0 <: HList](implicit vp: ValueProjectionAux[L, Out0]) = new ValueProjection[L] {
    type Out = Out0
    def apply(l: L): Out = vp(l)
  }
}

object ValueProjectionAux {
  implicit def valueProjectionHNil[K, V] = new ValueProjectionAux[(K, V) :: HNil, V :: HNil] {
    def apply(x: (K, V) :: HNil) = x.head._2 :: HNil
  }

  implicit def valueProjection[K, V, T <: HList](implicit st: ValueProjection[T]) = new ValueProjectionAux[(K, V) :: T, V :: st.Out] {
    def apply(x: (K, V) :: T) = x.head._2 :: st(x.tail)
  }
}

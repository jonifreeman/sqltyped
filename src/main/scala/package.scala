import shapeless._

package object sqltyped {
  import language.experimental.macros

  def sql[A, B](s: String)(implicit config: Configuration[A, B]) = macro SqlMacro.sqlImpl[A, B]

  implicit def assochlistOps[L <: HList](l: L): AssocHListOps[L] = new AssocHListOps(l)

  implicit def listOps[L <: HList](l: List[L]): ListOps[L] = new ListOps(l)  

  implicit def optionOps[L <: HList](l: Option[L]): OptionOps[L] = new OptionOps(l)  

  type @@[T, U] = TypeOperators.@@[T, U]

  def tag[U] = TypeOperators.tag[U]
}

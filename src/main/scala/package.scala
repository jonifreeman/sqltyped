import shapeless._

package object sqltyped {
  import language.experimental.macros

  def sql[A](s: String)(implicit config: Configuration[A]) = macro Sql.sqlImpl[A]

  implicit def assochlistOps[L <: HList](l: L): AssocHListOps[L] = new AssocHListOps(l)

  implicit def listOps[L <: HList](l: List[L]): ListOps[L] = new ListOps(l)  
}

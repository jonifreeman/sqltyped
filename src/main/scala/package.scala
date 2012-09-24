import shapeless._

package object sqltyped {
  import language.experimental.macros

  def sql[A, B](s: String)(implicit config: Configuration[A, B]) = macro SqlMacro.sqlImpl[A, B]

  def sqlt[A, B](s: String)(implicit config: Configuration[A, B]) = macro SqlMacro.sqltImpl[A, B]

  // FIXME switch to sql("select ...", keys = true) after;
  // https://issues.scala-lang.org/browse/SI-5920
  def sqlk[A, B](s: String)(implicit config: Configuration[A, B]) = macro SqlMacro.sqlkImpl[A, B]

  implicit def assochlistOps[L <: HList](l: L): AssocHListOps[L] = new AssocHListOps(l)

  implicit def listOps[L <: HList](l: List[L]): ListOps[L] = new ListOps(l)  

  implicit def optionOps[L <: HList](l: Option[L]): OptionOps[L] = new OptionOps(l)  

  type @@[T, U] = TypeOperators.@@[T, U]

  def tag[U] = TypeOperators.tag[U]

  private[sqltyped] type Result[A] = Either[String, A]
  private[sqltyped] def ok[A](a: A): Result[A] = Right(a)
  private[sqltyped] def fail[A](s: String): Result[A] = Left(s)
  private[sqltyped] implicit def rightBias[A](x: Result[A]): Either.RightProjection[String, A] = x.right
  private[sqltyped] implicit class ResultOps[A](a: A) {
    def ok = sqltyped.ok(a)
  }
}

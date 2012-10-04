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

  // Internally ? is used to denote computations that may fail.
  private[sqltyped] type ?[A] = Either[String, A]
  private[sqltyped] def ok[A](a: A): ?[A] = Right(a)
  private[sqltyped] def fail[A](s: String): ?[A] = Left(s)
  private[sqltyped] implicit def rightBias[A](x: ?[A]): Either.RightProjection[String, A] = x.right
  private[sqltyped] implicit class ResultOps[A](a: A) {
    def ok = sqltyped.ok(a)
  }
  private[sqltyped] implicit class ResultOptionOps[A](a: Option[A]) {
    def orFail(s: String) = a map sqltyped.ok getOrElse fail(s)
  }
  private[sqltyped] def sequence[A](rs: List[?[A]]): ?[List[A]] = 
    rs.foldRight(List[A]().ok) { (ra, ras) => for { as <- ras; a <- ra } yield a :: as }
  private[sqltyped] def sequenceO[A](rs: Option[?[A]]): ?[Option[A]] = 
    rs.foldRight(None.ok: ?[Option[A]]) { (ra, _) => for { a <- ra } yield Some(a) }
}

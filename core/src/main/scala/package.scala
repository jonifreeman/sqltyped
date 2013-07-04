import shapeless._

package object sqltyped {
  import language.experimental.macros

  def sql[A, B](s: String)(implicit config: Configuration[A, B]) = macro SqlMacro.sqlImpl[A, B]

  def sqlt[A, B](s: String)(implicit config: Configuration[A, B]) = macro SqlMacro.sqltImpl[A, B]

  // FIXME switch to sql("select ...", keys = true) after;
  // https://issues.scala-lang.org/browse/SI-5920
  def sqlk[A, B](s: String)(implicit config: Configuration[A, B]) = macro SqlMacro.sqlkImpl[A, B]

  def sqlj[A, B](s: String)(implicit config: Configuration[A, B]) = macro SqlMacro.sqljImpl[A, B]

  implicit class DynSQLContext(sc: StringContext) {
    def sql[A, B](exprs: Any*)(implicit config: Configuration[A, B]) = macro SqlMacro.dynsqlImpl[A, B]
  }

  implicit def recordOps[L <: HList](l: L): RecordOps[L] = new RecordOps(l)

  implicit def listOps[L <: HList](l: List[L]): ListOps[L] = new ListOps(l)  

  implicit def optionOps[L <: HList](l: Option[L]): OptionOps[L] = new OptionOps(l)  

  def keyAsString(k: Any) = {
    def isNumeric(s: String) = s.toCharArray.forall(Character.isDigit)

    if (k.getClass == classOf[String]) k.toString
    else {
      val parts = k.getClass.getName.split("\\$")
      parts.reverse.dropWhile(isNumeric).head
    }
  }

  // Internally ? is used to denote computations that may fail.
  private[sqltyped] def fail[A](s: String, column: Int = 0, line: Int = 0): ?[A] = 
    sqltyped.Failure(s, column, line)
  private[sqltyped] def ok[A](a: A): ?[A] = sqltyped.Ok(a)
  private[sqltyped] implicit class ResultOps[A](a: A) {
    def ok = sqltyped.ok(a)
  }
  private[sqltyped] implicit class ResultOptionOps[A](a: Option[A]) {
    def orFail(s: => String) = a map sqltyped.ok getOrElse fail(s)
  }
  private[sqltyped] def sequence[A](rs: List[?[A]]): ?[List[A]] = 
    rs.foldRight(List[A]().ok) { (ra, ras) => for { as <- ras; a <- ra } yield a :: as }
  private[sqltyped] def sequenceO[A](rs: Option[?[A]]): ?[Option[A]] = 
    rs.foldRight(None.ok: ?[Option[A]]) { (ra, _) => for { a <- ra } yield Some(a) }
}

package sqltyped {
  private[sqltyped] abstract sealed class ?[+A] { self =>
    def map[B](f: A => B): ?[B]
    def flatMap[B](f: A => ?[B]): ?[B]
    def foreach[U](f: A => U): Unit
    def fold[B](ifFail: Failure[A] => B, f: A => B): B
    def getOrElse[B >: A](default: => B): B
    def filter(p: A => Boolean): ?[A]
    def withFilter(p: A => Boolean): WithFilter = new WithFilter(p)
    class WithFilter(p: A => Boolean) {
      def map[B](f: A => B): ?[B] = self filter p map f
      def flatMap[B](f: A => ?[B]): ?[B] = self filter p flatMap f
      def foreach[U](f: A => U): Unit = self filter p foreach f
      def withFilter(q: A => Boolean): WithFilter = new WithFilter(x => p(x) && q(x))
    }
  }
  private[sqltyped] final case class Ok[+A](a: A) extends ?[A] {
    def map[B](f: A => B) = Ok(f(a))
    def flatMap[B](f: A => ?[B]) = f(a)
    def foreach[U](f: A => U) = { f(a); () }
    def fold[B](ifFail: Failure[A] => B, f: A => B) = f(a)
    def getOrElse[B >: A](default: => B) = a
    def filter(p: A => Boolean) = if (p(a)) this else fail("filter on ?[_] failed")
  }
  private[sqltyped] final case class Failure[+A](message: String, column: Int, line: Int) extends ?[A] {
    def map[B](f: A => B) = Failure(message, column, line)
    def flatMap[B](f: A => ?[B]) = Failure(message, column, line)
    def foreach[U](f: A => U) = ()
    def fold[B](ifFail: Failure[A] => B, f: A => B) = ifFail(this)
    def getOrElse[B >: A](default: => B) = default
    def filter(p: A => Boolean) = this
  }
}

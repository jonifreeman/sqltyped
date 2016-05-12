import shapeless._

package object sqltyped {
  import scala.annotation.StaticAnnotation
  import scala.language.experimental.macros
  import scala.language.implicitConversions

  class useInputTags extends StaticAnnotation
  class jdbcOnly extends StaticAnnotation
  class returnKeys extends StaticAnnotation
  class useSymbolKeyRecords extends StaticAnnotation

  def sql(s: String) = macro SqlMacro.sqlImpl

  @useInputTags def sqlt(s: String) = macro SqlMacro.sqlImpl

  // FIXME switch to sql("select ...", keys = true) after;
  // https://issues.scala-lang.org/browse/SI-5920
  @returnKeys def sqlk(s: String) = macro SqlMacro.sqlImpl

  @jdbcOnly def sqlj(s: String) = macro SqlMacro.sqlImpl

  @useSymbolKeyRecords def sqls(s: String) = macro SqlMacro.sqlImpl

  implicit class DynSQLContext(sc: StringContext) {
    def sql(exprs: Any*) = macro SqlMacro.dynsqlImpl
  }

  implicit class DynParamSQLContext(sc: StringContext) {
    def sqlp(exprs: Any*) = macro SqlMacro.paramDynsqlImpl
  }

  implicit def recordOps[R <: HList](r: R): RecordOps[R] = new RecordOps(r)  

  implicit def listOps[L <: HList](l: List[L]): ListOps[L] = new ListOps(l)  

  implicit def optionOps[L <: HList](l: Option[L]): OptionOps[L] = new OptionOps(l)  

  // To reduce importing when using records...
  implicit def mkSingletonOps(t: Any): syntax.SingletonOps = macro SingletonTypeMacros.mkSingletonOps

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

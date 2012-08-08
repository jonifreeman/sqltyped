package sqltyped

object AssocHList {
  import shapeless._

  implicit def assochlistOps[L <: HList](l: L): AssocHListOps[L] = new AssocHListOps(l)

  final class AssocHListOps[L <: HList](l: L) {
    def lookup[K](implicit lookup: Lookup[L, K]): lookup.Out = lookup(l)

    def get[K: Manifest](k: K)(implicit lookup0: Lookup[L, K]): lookup0.Out =
      lookup[K]
  }

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
}

case class Configuration[A](name: String, columns: List[A])

object Sql {
  import java.sql._
  import shapeless._
  import scala.reflect.makro._
  import language.experimental.macros

  /*
     import sqltyped._
     import Sql._
     object name
     object age
     val q = Query2[Int, name.type, age.type, String, Int]("select name,age from person", name, age, rs => "Joni", rs => 32)
     execute(q)
   */
  case class Query2[A, C1, C2, R1, R2](sql: String, c1: C1, c2: C2, r1: ResultSet => R1, r2: ResultSet => R2)

  def execute[A, C1, C2, R1, R2](q: Query2[A, C1, C2, R1, R2]): ((C1, R1) :: (C2, R2) :: HNil) = {
    val rs: ResultSet = null // exec sql here
    (q.c1 -> q.r1(rs)) :: (q.c2 -> q.r2(rs)) :: HNil
  }

  def sqlImpl[A: c.TypeTag](c: Context)(s: c.Expr[String])(config: c.Expr[Configuration[A]]): c.Expr[Any] = {
    import c.universe._

    val Literal(Constant(sql: String)) = s.tree
//    val Select(xx, termName) = config.tree

    // Select(config.tree, "name")
    // https://issues.scala-lang.org/browse/SI-5748
    // "It's more robust to parse the AST manually"
//    val cc = c.eval(c.Expr(c.resetAllAttrs(config.tree)))
//    println(cc.name)


/*          case x =>
        c.abort(c.enclosingPosition, "unexpected tree: " + show(x))
        */
    
//    if (sql.contains("id,name")) reify(Query[(Long, String)](s.splice))
//    else reify(Query[Long](s.splice))
//    c.Expr(Select(config.tree, "name"))
    c.Expr(Select(Select(config.tree, "columns"), "head"))
  }

  def sql[A](s: String)(implicit config: Configuration[A]) = macro sqlImpl[A]
}

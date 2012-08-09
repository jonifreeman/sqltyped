package sqltyped

case class Configuration[A](url: String, driver: String, username: String, password: String, columns: A)

object Sql {
  import java.sql._
  import shapeless._
  import scala.reflect.runtime.universe._
  import scala.reflect.makro._
  import language.experimental.macros

  case class Query2[C1, C2, R1, R2](sql: String, c1: C1, r1: ResultSet => R1, c2: C2, r2: ResultSet => R2)

  def query[C1, C2, R1, R2](q: Query2[C1, C2, R1, R2]): List[(C1, R1) :: (C2, R2) :: HNil] = {
    val rs: ResultSet = null // exec sql here
    List((q.c1 -> q.r1(rs)) :: (q.c2 -> q.r2(rs)) :: HNil)
  }

  def sqlImpl[A: c.TypeTag](c: Context)(s: c.Expr[String])(config: c.Expr[Configuration[A]]): c.Expr[Any] = {
    import c.universe._

    val Literal(Constant(sql: String)) = s.tree

    // https://issues.scala-lang.org/browse/SI-5748
    // "It's more robust to parse the AST manually"
//    val cc = c.eval(c.Expr(c.resetAllAttrs(config.tree)))
//    println(cc.name)

    val stmt = SqlParser.parse(sql)
    val meta = SqlMeta.infer(stmt)

    def rs(name: String, pos: Int) = 
      Function(List(ValDef(Modifiers(Flag.PARAM), newTermName("rs"), TypeTree(typeOf[java.sql.ResultSet]), EmptyTree)), Apply(Select(Ident(newTermName("rs")), newTermName(name)), List(Literal(Constant(pos)))))

    def col(name: String) = Select(Select(config.tree, "columns"), name)

    // FIXME create from meta
    c.Expr(Apply(Select(Select(Select(Ident("sqltyped"), newTermName("Sql")), newTermName("Query2")), newTermName("apply")), List(Literal(Constant(sql)), col("name"), rs("getString", 1), col("age"), rs("getInt", 2))))
  }

  def sql[A](s: String)(implicit config: Configuration[A]) = macro sqlImpl[A]

  case class SqlStmt(columnNames: List[String])

  object SqlParser {
    def parse(sql: String) = SqlStmt(List("name", "age")) // FIXME impl parsing
  }

  case class SqlMeta(columns: List[(String, Type)])

  object SqlMeta {
    def infer(stmt: SqlStmt) = SqlMeta(List(("name", typeOf[String]), ("age", typeOf[Int]))) // FIXME impl
  }

  implicit def assochlistOps[L <: HList](l: L): AssocHListOps[L] = new AssocHListOps(l)

  final class AssocHListOps[L <: HList](l: L) {
    def lookup[K](implicit lookup: Lookup[L, K]): lookup.Out = lookup(l)

    def get[K](k: K)(implicit lookup0: Lookup[L, K]): lookup0.Out = lookup[K]
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
}

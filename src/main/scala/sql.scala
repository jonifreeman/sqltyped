package sqltyped

case class Configuration[A](columns: A)

object Sql {
  import java.sql._
  import shapeless._
  import scala.reflect.runtime.universe._
  import scala.reflect.makro._
  import language.experimental.macros

  case class Query2[C1, C2, R1, R2](sql: String, c1: C1, r1: ResultSet => R1, c2: C2, r2: ResultSet => R2)

  // FIXME close stmt + rs
  def query[C1, C2, R1, R2](conn: Connection, q: Query2[C1, C2, R1, R2]): List[(C1, R1) :: (C2, R2) :: HNil] = {
    val stmt = conn.prepareStatement(q.sql)
    val rs = stmt.executeQuery
    val rows = collection.mutable.ListBuffer[(C1, R1) :: (C2, R2) :: HNil]()
    while (rs.next) {
      rows.append((q.c1 -> q.r1(rs)) :: (q.c2 -> q.r2(rs)) :: HNil)
    }
    rows.toList
  }

  def sqlImpl[A: c.TypeTag](c: Context)(s: c.Expr[String])(config: c.Expr[Configuration[A]]): c.Expr[Any] = {
    import c.universe._

    val Literal(Constant(sql: String)) = s.tree
    
    val url = System.getProperty("sqltyped.url")
    val driver = System.getProperty("sqltyped.driver")
    val username = System.getProperty("sqltyped.username")
    val password = System.getProperty("sqltyped.password")

    val stmt = SqlParser.parse(sql) fold (
      err => sys.error("Parse failed: " + err),
      res => SqlStmt(res)
    )
    val meta = Schema.infer(stmt, url, driver, username, password)

    def rs(name: String, pos: Int) = 
      Function(
        List(ValDef(Modifiers(Flag.PARAM), newTermName("rs"), TypeTree(typeOf[java.sql.ResultSet]), EmptyTree)), 
        Apply(Select(Ident(newTermName("rs")), newTermName(name)), List(Literal(Constant(pos)))))

    def col(name: String) = Select(Select(config.tree, "columns"), name)

    // FIXME date etc
    def rsGetterName(c: TypedColumn) = "get" + c.tpe.toString.capitalize

    val params = meta.columns.zipWithIndex.flatMap { case (c, i) => 
      List(col(c.column.name), rs(rsGetterName(c), i + 1)) 
    }
    c.Expr(Apply(Select(Select(Select(Ident("sqltyped"), newTermName("Sql")), newTermName("Query" + meta.columns.length)), newTermName("apply")), Literal(Constant(sql)) :: params))
  }

  def sql[A](s: String)(implicit config: Configuration[A]) = macro sqlImpl[A]

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

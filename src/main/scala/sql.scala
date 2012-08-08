package sqltyped

object AssocHList {
  import shapeless._

  final class AssocHListOps[L <: HList](l: L) {
    def lookup[K](implicit lookup: Lookup[L, K]): lookup.Out = lookup(l)
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
  import scala.reflect.makro._
  import language.experimental.macros

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
/*
    reify {
      Query2(s.splice, config.splice.name)
    } */
  }

  def sql[A](s: String)(implicit config: Configuration[A]) = macro sqlImpl[A]
}

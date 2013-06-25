package sqltyped

import org.scalatest._

class DynamicExamples extends MySQLConfig {
  test("Runtime query building") {
    val where = "age > ?" + " or " + "1 > 2"

    sql"select name from person where $where order by age".apply(Seq(5)) ===
      List("moe", "joe")

    sql"select name from person where $where order by age".apply(Seq(25)) ===
      List("joe")

    sql"select name, age from person where $where order by $orderBy".apply(Seq(5)).tuples ===
      List(("moe", 14), ("joe", 36))

    sql"select j.name, p.name from person p join job_history j on p.id=j.person where $where".apply(Seq(15)).tuples ===
      List(("Enron", "joe"), ("IBM", "joe"))
  }

  test("Simple expr library") {
    import ExprLib._

    val p1 = pred("age > ?", 15)
    val p2 = pred("age < ?", 2)
    val p3 = pred("length(name) < ?", 6)

    val expr = (p1 or p2) and p3

    sql"select name from person where ${expr.sql}".apply(expr.args) ===
      List("joe")
  }

  def orderBy = "age"

  object ExprLib {
    sealed trait Expr {
      def sql: String = this match {
        case Predicate(e, _) => e
        case And(l, r) => "(" + l.sql + " and " + r.sql + ")"
        case Or(l, r) => "(" + l.sql + " or " + r.sql + ")"
      }

      def args: Seq[Any] = this match {
        case Predicate(_, as) => as
        case And(l, r) => l.args ++ r.args
        case Or(l, r) => l.args ++ r.args
      }

      def and(other: Expr) = And(this, other)
      def or(other: Expr)  = Or(this, other)
    }

    case class Predicate(sqlExpr: String, arguments: Seq[Any]) extends Expr
    case class And(l: Expr, r: Expr) extends Expr
    case class Or(l: Expr, r: Expr) extends Expr

    def pred(sql: String, args: Any*) = Predicate(sql, args)
  }
}

package sqltyped

import scala.reflect.runtime.universe.{Type, typeOf}

private[sqltyped] object Ast {
  sealed trait Term
  sealed trait Value extends Term

  trait Aliased {
    def name: String
    def alias: Option[String]
    def aname = alias getOrElse name
  }

  case class Constant(tpe: Type, value: Any) extends Value
  case class Column(name: String, table: Option[String], alias: Option[String] = None) extends Value with Aliased
  case class Function(name: String, params: List[Term], alias: Option[String] = None) extends Value with Aliased

  case object Input extends Term
  case class Subselect(select: Select) extends Term

  case class Table(name: String, alias: Option[String])

  sealed trait Operator
  case object Eq extends Operator
  case object Neq extends Operator
  case object Lt extends Operator
  case object Gt extends Operator
  case object Le extends Operator
  case object Ge extends Operator

  sealed trait Expr
  case class Predicate(lhs: Term, op: Operator, rhs: Term) extends Expr
  case class And(e1: Expr, e2: Expr) extends Expr
  case class Or(e1: Expr, e2: Expr) extends Expr

  sealed trait Statement {
    def input: List[Value]
    def output: List[Value]
    def tables: List[Table]
    def toSql: String

    def tableOf(col: Column): Option[Table] = findTable { t => 
      (col.table, t.alias) match {
        case (Some(ref), None) => t.name == ref
        case (Some(ref), Some(a)) => t.name == ref || a == ref
        case (None, _) => true
      }
    }

    def findTable(p: Table => Boolean) = tables find p    
  }

  def params(e: Expr): List[Value] = e match {
    case Predicate(Input, op, x) => termToValue(x) :: Nil
    case Predicate(x, op, Input) => termToValue(x) :: Nil
    case Predicate(_, op, _)     => Nil
    case And(e1, e2)             => params(e1) ::: params(e2)
    case Or(e1, e2)              => params(e1) ::: params(e2)
  }

  // FIXME clean this
  def termToValue(x: Term) = x match {
    case v: Value => v
    case _ => sys.error("Invalid value " + x)
  }

  def format(col: Column): String = col.table.map(t => t + ".").getOrElse("") +
    col.name + col.alias.map(a => " as " + a).getOrElse("")
  
  def format(f: Function): String = f.name + "(" + (f.params map format).mkString(", ") + ")" + 
    f.alias.map(a => " as " + a).getOrElse("")

  def format(t: Table): String = t.name + t.alias.map(a => " as " + a).getOrElse("")

  def format(expr: Expr): String = expr match {
    case Predicate(t1, op, t2)   => format(t1) + " " + format(op) + " " + format(t2)
    case And(e1, e2)             => "(" + format(e1) + " and " + format(e2) + ")"
    case Or(e1, e2)              => "(" + format(e1) + " or " + format(e2) + ")"
  }

  def format(t: Term): String = t match {
    case Constant(tpe, v) => if (tpe == typeOf[String]) ("'" + v.toString + "'") else v.toString
    case col: Column => format(col)
    case f: Function => format(f)
    case Input => "?"
    case Subselect(select) => "(" + select.toSql + ")"
  }

  def format(v: Value): String = v match {
    case Constant(tpe, v) => if (tpe == typeOf[String]) ("'" + v.toString + "'") else v.toString
    case col: Column => format(col)
    case f: Function => format(f)
  }

  def format(op: Operator) = op match {
    case Eq  => "="
    case Neq => "!="
    case Lt  => "<"
    case Gt  => ">"
    case Le  => "<="
    case Ge  => ">="
  }

  def format(o: Order) = o match {
    case Asc => "asc"
    case Desc => "desc"
  }

  case class Select(projection: List[Value], 
                    from: List[From], // should be NonEmptyList
                    where: Option[Where], 
                    groupBy: Option[GroupBy],
                    orderBy: Option[OrderBy],
                    limit: Option[Limit]) extends Statement {

    def input = where.map(w => params(w.expr)).getOrElse(Nil) ::: groupBy.flatMap(g => g.having.map(h => params(h.expr))).getOrElse(Nil) // FIXME Limit
    def output = projection
    def tables = from flatMap { f => f.table :: f.join.map(_.table) }

    def toSql = "select " + (projection map format).mkString(", ") + " from " + 
      (from map (f => format(f.table) + 
                 f.join.map(j => " " + j.joinSpec + " " + format(j.table) + " on " + format(j.expr)).mkString(" "))).mkString(", ") +
      where.map(w => " where " + format(w.expr)).getOrElse("") +
      groupBy.map(g => 
        " group by " + format(g.col) + 
        (g.having.map(h => " having " + format(h.expr)).getOrElse(""))).getOrElse("") +
      orderBy.map(o => " order by " + (o.cols map format).mkString(", ") + o.order.map(ord => " " + format(ord)).getOrElse("")).getOrElse("")
  }

  case class From(table: Table, join: List[Join])

  case class Where(expr: Expr)

  case class Join(table: Table, expr: Expr, joinSpec: String)

  case class GroupBy(col: Column, having: Option[Having])

  case class Having(expr: Expr)

  case class OrderBy(cols: List[Column], order: Option[Order])

  sealed trait Order
  case object Asc extends Order
  case object Desc extends Order

  case class Limit(count: Int, offset: Option[Int])
}

package sqltyped

import schemacrawler.schema.Schema
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
  case class Column(name: String, table: Option[String], alias: Option[String] = None, 
                    resolvedTable: Option[Table] = None) extends Value with Aliased
  case class AllColumns(table: Option[String], resolvedTable: Option[Table] = None) extends Value
  case class Function(name: String, params: List[Term], alias: Option[String] = None) extends Value with Aliased
  case class ArithExpr(lhs: Value, op: String, rhs: Value) extends Value
  
  case object Input extends Term
  case class Subselect(select: Select) extends Term

  case class Table(name: String, alias: Option[String])

  sealed trait Operator1
  case object IsNull extends Operator1
  case object IsNotNull extends Operator1

  sealed trait Operator2
  case object Eq extends Operator2
  case object Neq extends Operator2
  case object Lt extends Operator2
  case object Gt extends Operator2
  case object Le extends Operator2
  case object Ge extends Operator2
  case object In extends Operator2

  sealed trait Operator3
  case object Between extends Operator3

  sealed trait Expr {
    def find(p: Expr => Boolean): Option[Expr] = 
      if (p(this)) Some(this)
      else this match {
        case And(e1, e2)  => e1.find(p) orElse e2.find(p)
        case Or(e1, e2)   => e1.find(p) orElse e2.find(p)
        case _: Predicate => None
      }
  }

  sealed trait Predicate extends Expr

  case class Predicate1(term: Term, op: Operator1) extends Predicate
  case class Predicate2(lhs: Term, op: Operator2, rhs: Term) extends Predicate
  case class Predicate3(t1: Term, op: Operator3, t2: Term, t3: Term) extends Predicate
  case class And(e1: Expr, e2: Expr) extends Expr
  case class Or(e1: Expr, e2: Expr) extends Expr

  sealed trait Statement {
    def input(schema: Schema): List[Value]
    def output: List[Value]
    def tables: List[Table]
    def isQuery = false
    def toSql: String

    /**
     * Returns a Statement where all columns have their tables resolved.
     */
    def resolveTables: Statement
  }

  private class ResolveEnv(env: List[Table]) {
    def resolve(term: Term): Term = term match {
      case col: Column => resolveColumn(col)
      case AllColumns(t, _) => resolveAllColumns(t)
      case f@Function(_, ps, _) => resolveFunc(f)
      case Subselect(select) => Subselect(resolveSelect(select)(select.tables ::: env))
      case ArithExpr(lhs, op, rhs) => ArithExpr(resolveValue(lhs), op, resolveValue(rhs))
      case t => t
    }

    def resolveColumn(col: Column) = 
      env find { t => 
        (col.table, t.alias) match {
          case (Some(ref), None) => t.name == ref
          case (Some(ref), Some(a)) => t.name == ref || a == ref
          case (None, _) => true
        }
      } map(t => col.copy(resolvedTable = Some(t))) getOrElse sys.error("Column references unknown table " + col)

    def resolveAllColumns(tableRef: Option[String]) = AllColumns(tableRef, tableRef
      .map(ref => env.find(t => t.name == ref) getOrElse sys.error("Unknown table " + ref))
      .orElse(env.headOption))

    def resolveValue(v: Value): Value = v match {
      case col: Column => resolveColumn(col)
      case AllColumns(t, _) => resolveAllColumns(t)
      case f: Function => resolveFunc(f)
      case ArithExpr(lhs, op, rhs) => ArithExpr(resolveValue(lhs), op, resolveValue(rhs))
      case x => x
    }

    def resolveFunc(f: Function) = f.copy(params = f.params map resolve)
    def resolveProj(proj: List[Value]) = proj map resolveValue
    def resolveFroms(from: List[From]) = from map resolveFrom
    def resolveFrom(from: From) = from.copy(join = from.join map resolveJoin)
    def resolveJoin(join: Join) = join.copy(expr = resolveExpr(join.expr))
    def resolveWhere(where: Option[Where]) = where.map(w => Where(resolveExpr(w.expr)))
    def resolveGroupBy(groupBy: Option[GroupBy]) = groupBy.map(g => GroupBy(resolveColumn(g.col), resolveHaving(g.having)))
    def resolveHaving(having: Option[Having]) = having.map(h => Having(resolveExpr(h.expr)))
    def resolveOrderBy(orderBy: Option[OrderBy]) = orderBy.map(o => o.copy(cols = o.cols map resolveColumn))

    def resolveExpr(e: Expr): Expr = e match {
      case p@Predicate1(t1, op)         => p.copy(term = resolve(t1))
      case p@Predicate2(t1, op, t2)     => p.copy(lhs = resolve(t1), rhs = resolve(t2))
      case p@Predicate3(t1, op, t2, t3) => p.copy(t1 = resolve(t1), t2 = resolve(t2), t3 = resolve(t3))
      case And(e1, e2)                  => And(resolveExpr(e1), resolveExpr(e2))
      case Or(e1, e2)                   => Or(resolveExpr(e1), resolveExpr(e2))
    }
  }

  private def resolveSelect(s: Select)(env: List[Table] = s.tables): Select = {
    val r = new ResolveEnv(env)
    s.copy(projection = r.resolveProj(s.projection), 
           from = r.resolveFroms(s.from), 
           where = r.resolveWhere(s.where), 
           groupBy = r.resolveGroupBy(s.groupBy), 
           orderBy = r.resolveOrderBy(s.orderBy))
  }

  private def resolveDelete(d: Delete)(env: List[Table] = d.tables): Delete = {
    val r = new ResolveEnv(env)
    d.copy(from = d.from.map(f => r.resolveFrom(f)), 
           where = r.resolveWhere(d.where))
  }

  private def resolveUpdate(u: Update)(env: List[Table] = u.tables): Update = {
    val r = new ResolveEnv(env)
    u.copy(set = u.set map { case (c, t) => (r.resolveColumn(c), r.resolve(t)) },
           where = r.resolveWhere(u.where),
           orderBy = r.resolveOrderBy(u.orderBy))
  }

  def params(e: Expr): List[Value] = e match {
    case Predicate1(_, _)                => Nil
    case Predicate2(Input, op, x)        => termToValue(x) :: Nil
    case Predicate2(x, op, Input)        => termToValue(x) :: Nil
    case Predicate2(_, op, Subselect(s)) => s.where.map(w => params(w.expr)).getOrElse(Nil) // FIXME groupBy
    case Predicate2(_, op, _)            => Nil
    case Predicate3(x, op, Input, Input) => termToValue(x) :: termToValue(x) :: Nil
    case Predicate3(x, op, Input, _)     => termToValue(x) :: Nil
    case Predicate3(x, op, _, Input)     => termToValue(x) :: Nil
    case Predicate3(_, op, _, _)         => Nil
    case And(e1, e2)                     => params(e1) ::: params(e2)
    case Or(e1, e2)                      => params(e1) ::: params(e2)
  }

  def limitParams(limit: Option[Limit]) =
    limit.map(l => l.count.right.toSeq.toList ::: l.offset.map(_.right.toSeq.toList).getOrElse(Nil))
      .getOrElse(Nil).map(_ => Constant(typeOf[Long], None))

  // FIXME clean this
  def termToValue(x: Term) = x match {
    case v: Value => v
    case _ => sys.error("Invalid value " + x)
  }

  def format(col: Column): String = col.table.map(t => t + ".").getOrElse("") +
    col.name + col.alias.map(a => " as " + a).getOrElse("")
  
  def format(f: Function): String = f.name + "(" + (f.params map format).mkString(", ") + ")" + 
    f.alias.map(a => " as " + a).getOrElse("")

  def format(a: ArithExpr): String = "(" + format(a.lhs) + " " + a.op + " " + format(a.rhs) + ")"

  def format(t: Table): String = t.name + t.alias.map(a => " as " + a).getOrElse("")

  def format(expr: Expr): String = expr match {
    case Predicate1(t, a)           => format(t) + " " + format(a)
    case Predicate2(t1, op, t2)     => format(t1) + " " + format(op) + " " + format(t2)
    case Predicate3(t1, op, t2, t3) => format(t1) + " " + format(op) + " " + format(t2) + " and " + format(t3)
    case And(e1, e2)                => "(" + format(e1) + " and " + format(e2) + ")"
    case Or(e1, e2)                 => "(" + format(e1) + " or " + format(e2) + ")"
  }

  def format(t: Term): String = t match {
    case Constant(tpe, v) => if (tpe == typeOf[String]) ("'" + v.toString + "'") else v.toString
    case col: Column => format(col)
    case a: ArithExpr => format(a)
    case AllColumns(None, _) => "*"
    case AllColumns(Some(t), _) => "*." + t
    case f: Function => format(f)
    case Input => "?"
    case Subselect(select) => "(" + select.toSql + ")"
  }

  def format(v: Value): String = v match {
    case Constant(tpe, v) => if (tpe == typeOf[String]) ("'" + v.toString + "'") else v.toString
    case col: Column => format(col)
    case a: ArithExpr => format(a)
    case AllColumns(None, _) => "*"
    case AllColumns(Some(t), _) => "*." + t
    case f: Function => format(f)
  }

  def format(op: Operator1) = op match {
    case IsNull    => "is null"
    case IsNotNull => "is not null"
  }

  def format(op: Operator2) = op match {
    case Eq       => "="
    case Neq      => "!="
    case Lt       => "<"
    case Gt       => ">"
    case Le       => "<="
    case Ge       => ">="
    case In       => "in"
  }

  def format(op: Operator3) = op match {
    case Between  => "between"
  }

  def format(o: Order) = o match {
    case Asc  => "asc"
    case Desc => "desc"
  }

  case class Delete(from: List[From], where: Option[Where]) extends Statement {
    def input(schema: Schema) = where.map(w => params(w.expr)).getOrElse(Nil)
    def output = Nil
    def tables = from.map(_.table)
    def resolveTables = resolveDelete(this)()

    def toSql = 
      "delete from " + from.map(_.table.name).mkString(",") + 
      where.map(w => " where " + format(w.expr)).getOrElse("")
  }

  sealed trait InsertInput
  case class ListedInput(values: List[Term]) extends InsertInput
  case class SelectedInput(select: Select) extends InsertInput

  case class Insert(table: Table, colNames: Option[List[String]], insertInput: InsertInput) extends Statement {
    def input(schema: Schema) = {
      def colNamesFromSchema = schema.getTable(table.name).getColumns.toList.map(_.getName)

      insertInput match {
        case ListedInput(values) => 
          colNames getOrElse colNamesFromSchema zip values collect {
            case (name, Input) => Column(name, None, None, Some(table))
          }
        case SelectedInput(select) => select.input(schema)
      }
    }

    def output = Nil
    def tables = table :: Nil
    def resolveTables = copy(insertInput = insertInput match {
      case SelectedInput(select) => SelectedInput(select.resolveTables)
      case i => i
    })

    def toSql = 
      "insert into " + table.name + colNames.map(" (" + _.mkString(", ") + ")").getOrElse("") 
    //  " values (" + (values map format) + ")"
  }

  case class Union(left: Statement, right: Statement, 
                   orderBy: Option[OrderBy], limit: Option[Limit]) extends Statement {
    def tables = left.tables ::: right.tables
    def input(schema: Schema) = left.input(schema) ::: right.input(schema) ::: limitParams(limit)
    def output = left.output
    def resolveTables = Union(left.resolveTables, right.resolveTables, orderBy, limit)
    override def isQuery = true

    def toSql = 
      "(" + left.toSql + ") union (" + right.toSql + ")" +
      orderBy.map(o => " order by " + (o.cols map format).mkString(", ") + o.order.map(ord => " " + format(ord)).getOrElse("")).getOrElse("")
  }

  case class Update(tables: List[Table], set: List[(Column, Term)], where: Option[Where], 
                    orderBy: Option[OrderBy], limit: Option[Limit]) extends Statement {
    def input(schema: Schema) = 
      set.collect { case (col, Input) => col } :::
      where.map(w => params(w.expr)).getOrElse(Nil) ::: 
      limitParams(limit)

    def output = Nil
    def resolveTables = resolveUpdate(this)()

    def toSql = 
      "update " + tables.map(_.name).mkString(", ") + " set " + 
      (set.map { case (c, v) => format(c) + "=" + format(v) }).mkString(", ") +
      where.map(w => " where " + format(w.expr)).getOrElse("") +
      orderBy.map(o => " order by " + (o.cols map format).mkString(", ") + o.order.map(ord => " " + format(ord)).getOrElse("")).getOrElse("")
  }

  case object Create extends Statement {
    def input(schema: Schema) = Nil
    def output = Nil
    def tables = Nil
    def resolveTables = this
    def toSql = "create"      
  }

  case class Select(projection: List[Value], 
                    from: List[From], // should be NonEmptyList
                    where: Option[Where], 
                    groupBy: Option[GroupBy],
                    orderBy: Option[OrderBy],
                    limit: Option[Limit]) extends Statement {

    def input(schema: Schema) = 
      where.map(w => params(w.expr)).getOrElse(Nil) ::: 
      groupBy.flatMap(g => g.having.map(h => params(h.expr))).getOrElse(Nil) :::
      limitParams(limit)

    def output = projection
    def tables = from flatMap (_.tables)
    def resolveTables = resolveSelect(this)()
    override def isQuery = true

    def toSql = 
      "select " + (projection map format).mkString(", ") + " from " + 
      (from map (f => format(f.table) + 
                 f.join.map(j => " " + j.joinSpec + " " + format(j.table) + " on " + format(j.expr)).mkString(" "))).mkString(", ") +
      where.map(w => " where " + format(w.expr)).getOrElse("") +
      groupBy.map(g => 
        " group by " + format(g.col) + 
        (g.having.map(h => " having " + format(h.expr)).getOrElse(""))).getOrElse("") +
      orderBy.map(o => " order by " + (o.cols map format).mkString(", ") + o.order.map(ord => " " + format(ord)).getOrElse("")).getOrElse("")
  }

  case class From(table: Table, join: List[Join]) {
    def tables = table :: join.map(_.table)
  }

  case class Where(expr: Expr)

  case class Join(table: Table, expr: Expr, joinSpec: String)

  case class GroupBy(col: Column, having: Option[Having])

  case class Having(expr: Expr)

  case class OrderBy(cols: List[Column], order: Option[Order])

  sealed trait Order
  case object Asc extends Order
  case object Desc extends Order

  case class Limit(count: Either[Int, Input.type], offset: Option[Either[Int, Input.type]])
}

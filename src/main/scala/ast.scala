package sqltyped

import schemacrawler.schema.Schema
import scala.reflect.runtime.universe.{Type, typeOf}

private[sqltyped] object Ast {
  sealed trait Term[T]
  sealed trait Value[T] extends Term[T]

  trait Aliased {
    def name: String
    def alias: Option[String]
    def aname = alias getOrElse name
  }

  case class Constant[T](tpe: Type, value: Any) extends Value[T]
  case class Column[T](name: String, table: T, alias: Option[String] = None) extends Value[T] with Aliased
  case class AllColumns[T](table: T) extends Value[T]
  case class Function[T](name: String, params: List[Term[T]], alias: Option[String] = None) extends Value[T] with Aliased
  case class ArithExpr[T](lhs: Value[T], op: String, rhs: Value[T]) extends Value[T]
  
  case class Input[T] extends Term[T]
  case class Subselect[T](select: Select[T]) extends Term[T]

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

  sealed trait Expr[T] {
    def find(p: Expr[T] => Boolean): Option[Expr[T]] = 
      if (p(this)) Some(this)
      else this match {
        case And(e1, e2)  => e1.find(p) orElse e2.find(p)
        case Or(e1, e2)   => e1.find(p) orElse e2.find(p)
        case _: Predicate[T] => None
      }
  }

  sealed trait Predicate[T] extends Expr[T]

  case class Predicate1[T](term: Term[T], op: Operator1) extends Predicate[T]
  case class Predicate2[T](lhs: Term[T], op: Operator2, rhs: Term[T]) extends Predicate[T]
  case class Predicate3[T](t1: Term[T], op: Operator3, t2: Term[T], t3: Term[T]) extends Predicate[T]
  case class And[T](e1: Expr[T], e2: Expr[T]) extends Expr[T]
  case class Or[T](e1: Expr[T], e2: Expr[T]) extends Expr[T]

  // Parametrized by Table type (Option[String] or Table)
  sealed trait Statement[T] {
    def tables: List[Table]
    def isQuery = false
  }

  def input(schema: Schema, stmt: Statement[Table]): List[Value[Table]] = stmt match {
    case Delete(from, where) => where.map(w => params(w.expr)).getOrElse(Nil)

    case Insert(table, colNames, insertInput) =>
      def colNamesFromSchema = schema.getTable(table.name).getColumns.toList.map(_.getName)

      insertInput match {
        case ListedInput(values) => 
          colNames getOrElse colNamesFromSchema zip values collect {
            case (name, Input()) => Column(name, table, None)
          }
        case SelectedInput(select) => input(schema, select)
      }

    case Union(left, right, orderBy, limit) => 
      input(schema, left) ::: input(schema, right) ::: limitParams(limit)

    case Update(tables, set, where, orderBy, limit) => 
      set.collect { case (col, Input()) => col } :::
      where.map(w => params(w.expr)).getOrElse(Nil) ::: 
      limitParams(limit)

    case Create() => Nil

    case Select(projection, from, where, groupBy, orderBy, limit) =>
      where.map(w => params(w.expr)).getOrElse(Nil) ::: 
      groupBy.flatMap(g => g.having.map(h => params(h.expr))).getOrElse(Nil) :::
      limitParams(limit)
  }

  def output(stmt: Statement[Table]): List[Value[Table]] = stmt match {
    case Delete(_, _) => Nil
    case Insert(_, _, _) => Nil
    case Union(left, _, _, _) => output(left)
    case Update(_, _, _, _, _) => Nil
    case Create() => Nil
    case Select(projection, _, _, _, _, _) => projection
  }

  /**
   * Returns a Statement where all columns have their tables resolved.
   */
  def resolveTables(stmt: Statement[Option[String]]): Result[Statement[Table]] = stmt match {
    case s@Select(_, _, _, _, _, _) => resolveSelect(s)()
    case d@Delete(_, _) => resolveDelete(d)()
    case u@Update(_, _, _, _, _) => resolveUpdate(u)()
    case Create() => Create[Table]().ok
    case i@Insert(_, _, _) => resolveInsert(i)()
    case u@Union(_, _, _, _) => resolveUnion(u)()
  }

  private class ResolveEnv(env: List[Table]) {
    def resolve(term: Term[Option[String]]): Result[Term[Table]] = term match {
      case col@Column(_, _, _) => resolveColumn(col)
      case AllColumns(t) => resolveAllColumns(t)
      case f@Function(_, ps, _) => resolveFunc(f)
      case Subselect(select) => resolveSelect(select)(select.tables ::: env) map (s => Subselect(s))
      case ArithExpr(lhs, op, rhs) => 
        for { l <- resolveValue(lhs); r <- resolveValue(rhs) } yield ArithExpr(l, op, r)
      case Constant(tpe, value) => Constant[Table](tpe, value).ok
      case Input() => Input[Table]().ok
    }

    def resolveColumn(col: Column[Option[String]]) = 
      env find { t => 
        (col.table, t.alias) match {
          case (Some(ref), None) => t.name == ref
          case (Some(ref), Some(a)) => t.name == ref || a == ref
          case (None, _) => true
        }
      } map (t => col.copy(table = t)) resultOrFail ("Column references unknown table " + col)

    def resolveAllColumns(tableRef: Option[String]) = tableRef match {
      case Some(ref) => 
        (env.find(t => t.name == ref) resultOrFail ("Unknown table " + ref)) map (r => AllColumns(r))
      case None => 
        AllColumns(env.head).ok
    }

    def resolveValue(v: Value[Option[String]]): Result[Value[Table]] = v match {
      case col@Column(_, _, _) => resolveColumn(col)
      case AllColumns(t) => resolveAllColumns(t)
      case f@Function(_, _, _) => resolveFunc(f)
      case ArithExpr(lhs, op, rhs) => 
        for { l <- resolveValue(lhs); r <- resolveValue(rhs) } yield ArithExpr(l, op, r)
      case Constant(tpe, value) => Constant[Table](tpe, value).ok
    }

    def resolveFunc(f: Function[Option[String]]) = sequence(f.params map resolve) map (ps => f.copy(params = ps))
    def resolveProj(proj: List[Value[Option[String]]]) = sequence(proj map resolveValue)
    def resolveFroms(from: List[From[Option[String]]]) = sequence(from map resolveFrom)
    def resolveFrom(from: From[Option[String]]) = sequence(from.join map resolveJoin) map (j => from.copy(join = j))
    def resolveJoin(join: Join[Option[String]]) = resolveExpr(join.expr) map (e => join.copy(expr = e))
    def resolveWhere(where: Where[Option[String]]) = resolveExpr(where.expr) map Where.apply
    def resolveWhereOpt(where: Option[Where[Option[String]]]) = sequenceO(where map resolveWhere)
    def resolveGroupBy(groupBy: GroupBy[Option[String]]) = for {
      c <- resolveColumn(groupBy.col)
      h <- resolveHavingOpt(groupBy.having)
    } yield groupBy.copy(col = c, having = h)
    def resolveGroupByOpt(groupBy: Option[GroupBy[Option[String]]]) = sequenceO(groupBy map resolveGroupBy)
    def resolveHaving(having: Having[Option[String]]) = resolveExpr(having.expr) map Having.apply
    def resolveHavingOpt(having: Option[Having[Option[String]]]) = sequenceO(having map resolveHaving)
    def resolveOrderBy(orderBy: OrderBy[Option[String]]) = sequence(orderBy.cols map resolveColumn) map (cs => orderBy.copy(cols = cs))
    def resolveOrderByOpt(orderBy: Option[OrderBy[Option[String]]]) = sequenceO(orderBy map resolveOrderBy)
    def resolveLimit(limit: Limit[Option[String]]) = 
      Limit[Table](
        limit.count.right map (_ => Input[Table]()),
        limit.offset.map(_.right map (_ => Input[Table]()))
      ).ok
    def resolveLimitOpt(limit: Option[Limit[Option[String]]]) = sequenceO(limit map resolveLimit)

    def resolveExpr(e: Expr[Option[String]]): Result[Expr[Table]] = e match {
      case p@Predicate1(t1, op) => 
        resolve(t1) map (t => p.copy(term = t))
      case p@Predicate2(t1, op, t2) => 
        for { l <- resolve(t1); r <- resolve(t2) } yield p.copy(lhs = l, rhs = r)
      case p@Predicate3(t1, op, t2, t3) => 
        for { r1 <- resolve(t1); r2 <- resolve(t2); r3 <- resolve(t3) } yield p.copy(t1 = r1, t2 = r2, t3 = r3)
      case And(e1, e2) => 
        for { r1 <- resolveExpr(e1); r2 <- resolveExpr(e2) } yield And(r1, r2)
      case Or(e1, e2) =>
        for { r1 <- resolveExpr(e1); r2 <- resolveExpr(e2) } yield Or(r1, r2)
    }
  }

  private def resolveSelect(s: Select[Option[String]])(env: List[Table] = s.tables): Result[Select[Table]] = {
    val r = new ResolveEnv(env)
    for {
      p <- r.resolveProj(s.projection)
      f <- r.resolveFroms(s.from)
      w <- r.resolveWhereOpt(s.where) 
      g <- r.resolveGroupByOpt(s.groupBy) 
      o <- r.resolveOrderByOpt(s.orderBy)
      l <- r.resolveLimitOpt(s.limit)
    } yield s.copy(projection = p, from = f, where = w, groupBy = g, orderBy = o, limit = l)
  }

  private def resolveInsert(i: Insert[Option[String]])(env: List[Table] = i.tables): Result[Insert[Table]] = {
    val r = new ResolveEnv(env)
    (i.insertInput match {
      case SelectedInput(select) => resolveSelect(select)() map SelectedInput.apply
      case ListedInput(vals) => sequence(vals map r.resolve) map ListedInput.apply
    }) map (in => i.copy(insertInput = in))
  }

  private def resolveUnion(u: Union[Option[String]])(env: List[Table] = u.tables): Result[Union[Table]] = {
    val r = new ResolveEnv(env)
    for {
      le <- resolveTables(u.left)
      ri <- resolveTables(u.right)
      o  <- r.resolveOrderByOpt(u.orderBy)
      l  <- r.resolveLimitOpt(u.limit)
    } yield Union(le, ri, o, l)
  }

  private def resolveDelete(d: Delete[Option[String]])(env: List[Table] = d.tables): Result[Delete[Table]] = {
    val r = new ResolveEnv(env)
    for {
      f <- sequence(d.from.map(f => r.resolveFrom(f)))
      w <- r.resolveWhereOpt(d.where)
    } yield d.copy(from = f, where = w)
  }

  private def resolveUpdate(u: Update[Option[String]])(env: List[Table] = u.tables): Result[Update[Table]] = {
    val r = new ResolveEnv(env)

    def resolveSet(c: Column[Option[String]], t: Term[Option[String]]) = for { 
      rc <- r.resolveColumn(c)
      rt <- r.resolve(t) 
    } yield (rc, rt)

    for {
      s <- sequence(u.set map { case (c, t) => resolveSet(c, t) })
      w <- r.resolveWhereOpt(u.where)
      o <- r.resolveOrderByOpt(u.orderBy)
      l <- r.resolveLimitOpt(u.limit)
    } yield u.copy(set = s, where = w, orderBy = o, limit = l)
  }

  def params[T](e: Expr[T]): List[Value[T]] = e match {
    case Predicate1(_, _)                    => Nil
    case Predicate2(Input(), op, x)          => termToValue(x) :: Nil
    case Predicate2(x, op, Input())          => termToValue(x) :: Nil
    case Predicate2(_, op, Subselect(s))     => s.where.map(w => params(w.expr)).getOrElse(Nil) // FIXME groupBy
    case Predicate2(_, op, _)                => Nil
    case Predicate3(x, op, Input(), Input()) => termToValue(x) :: termToValue(x) :: Nil
    case Predicate3(x, op, Input(), _)       => termToValue(x) :: Nil
    case Predicate3(x, op, _, Input())       => termToValue(x) :: Nil
    case Predicate3(_, op, _, _)             => Nil
    case And(e1, e2)                         => params(e1) ::: params(e2)
    case Or(e1, e2)                          => params(e1) ::: params(e2)
  }

  def limitParams[T](limit: Option[Limit[T]]) =
    limit.map(l => l.count.right.toSeq.toList ::: l.offset.map(_.right.toSeq.toList).getOrElse(Nil))
      .getOrElse(Nil).map(_ => Constant[T](typeOf[Long], None))

  // FIXME clean this
  def termToValue[T](x: Term[T]) = x match {
    case v: Value[T] => v
    case _ => sys.error("Invalid value " + x)
  }

  case class Delete[T](from: List[From[T]], where: Option[Where[T]]) extends Statement[T] {
    def tables = from.map(_.table)
  }

  sealed trait InsertInput[T]
  case class ListedInput[T](values: List[Term[T]]) extends InsertInput[T]
  case class SelectedInput[T](select: Select[T]) extends InsertInput[T]

  case class Insert[T](table: Table, colNames: Option[List[String]], insertInput: InsertInput[T]) extends Statement[T] {
    def output = Nil
    def tables = table :: Nil
  }

  case class Union[T](left: Statement[T], right: Statement[T], 
                      orderBy: Option[OrderBy[T]], limit: Option[Limit[T]]) extends Statement[T] {
    def tables = left.tables ::: right.tables
    override def isQuery = true
  }

  case class Update[T](tables: List[Table], set: List[(Column[T], Term[T])], where: Option[Where[T]], 
                       orderBy: Option[OrderBy[T]], limit: Option[Limit[T]]) extends Statement[T]

  case class Create[T] extends Statement[T] {
    def tables = Nil
  }

  case class Select[T](projection: List[Value[T]], 
                       from: List[From[T]], // should be NonEmptyList
                       where: Option[Where[T]], 
                       groupBy: Option[GroupBy[T]],
                       orderBy: Option[OrderBy[T]],
                       limit: Option[Limit[T]]) extends Statement[T] {
    def tables = from flatMap (_.tables)
    override def isQuery = true
  }

  case class From[T](table: Table, join: List[Join[T]]) {
    def tables = table :: join.map(_.table)
  }

  case class Where[T](expr: Expr[T])

  case class Join[T](table: Table, expr: Expr[T], joinSpec: String)

  case class GroupBy[T](col: Column[T], having: Option[Having[T]])

  case class Having[T](expr: Expr[T])

  case class OrderBy[T](cols: List[Column[T]], order: Option[Order])

  sealed trait Order
  case object Asc extends Order
  case object Desc extends Order

  case class Limit[T](count: Either[Int, Input[T]], offset: Option[Either[Int, Input[T]]])
}

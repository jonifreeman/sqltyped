package sqltyped

import schemacrawler.schema.Schema
import scala.reflect.runtime.universe.{Type, typeOf}

private[sqltyped] object Ast {
  // Types used for AST when references to tables are not yet resolved 
  // (table is optional string reference).
  trait Unresolved {
    type Expr           = Ast.Expr[Option[String]]
    type Term           = Ast.Term[Option[String]]
    type Named          = Ast.Named[Option[String]]
    type Statement      = Ast.Statement[Option[String]]
    type ArithExpr      = Ast.ArithExpr[Option[String]]
    type Comparison     = Ast.Comparison[Option[String]]
    type Column         = Ast.Column[Option[String]]
    type Function       = Ast.Function[Option[String]]
    type Constant       = Ast.Constant[Option[String]]
    type Select         = Ast.Select[Option[String]]
    type Join           = Ast.Join[Option[String]]
    type JoinType       = Ast.JoinType[Option[String]]
    type TableReference = Ast.TableReference[Option[String]]
    type ConcreteTable  = Ast.ConcreteTable[Option[String]]
    type DerivedTable   = Ast.DerivedTable[Option[String]]
    type Where          = Ast.Where[Option[String]]
    type Limit          = Ast.Limit[Option[String]]
  }
  object Unresolved extends Unresolved

  // Types used for AST when references to tables are resolved 
  trait Resolved {
    type Expr           = Ast.Expr[Table]
    type Term           = Ast.Term[Table]
    type Named          = Ast.Named[Table]
    type Statement      = Ast.Statement[Table]
    type ArithExpr      = Ast.ArithExpr[Table]
    type Comparison     = Ast.Comparison[Table]
    type Column         = Ast.Column[Table]
    type Function       = Ast.Function[Table]
    type Constant       = Ast.Constant[Table]
    type Select         = Ast.Select[Table]
    type Join           = Ast.Join[Table]
    type JoinType       = Ast.JoinType[Table]
    type TableReference = Ast.TableReference[Table]
    type ConcreteTable  = Ast.ConcreteTable[Table]
    type DerivedTable   = Ast.DerivedTable[Table]
    type Where          = Ast.Where[Table]
    type Limit          = Ast.Limit[Table]
  }
  object Resolved extends Resolved

  sealed trait Term[T]

  case class Named[T](name: String, alias: Option[String], term: Term[T]) {
    def aname = alias getOrElse name
  }

  case class Constant[T](tpe: Type, value: Any) extends Term[T]
  case class Column[T](name: String, table: T) extends Term[T]
  case class AllColumns[T](table: T) extends Term[T]
  case class Function[T](name: String, params: List[Expr[T]]) extends Term[T]
  case class ArithExpr[T](lhs: Term[T], op: String, rhs: Term[T]) extends Term[T]  
  case class Input[T]() extends Term[T]
  case class Subselect[T](select: Select[T]) extends Term[T]
  case class TermList[T](terms: List[Term[T]]) extends Term[T]

  case class Table(name: String, alias: Option[String])

  sealed trait Operator1
  case object IsNull extends Operator1
  case object IsNotNull extends Operator1
  case object Exists extends Operator1
  case object NotExists extends Operator1

  sealed trait Operator2
  case object Eq extends Operator2
  case object Neq extends Operator2
  case object Lt extends Operator2
  case object Gt extends Operator2
  case object Le extends Operator2
  case object Ge extends Operator2
  case object In extends Operator2
  case object NotIn extends Operator2
  case object Like extends Operator2

  sealed trait Operator3
  case object Between extends Operator3
  case object NotBetween extends Operator3

  sealed trait Expr[T] {
    def find(p: Expr[T] => Boolean): Option[Expr[T]] = 
      if (p(this)) Some(this)
      else this match {
        case And(e1, e2) => e1.find(p) orElse e2.find(p)
        case Or(e1, e2)  => e1.find(p) orElse e2.find(p)
        case _ => None
      }
  }

  sealed trait Comparison[T] extends Expr[T] with Term[T]

  case class SimpleExpr[T](term: Term[T]) extends Expr[T]
  case class Comparison1[T](term: Term[T], op: Operator1) extends Comparison[T]
  case class Comparison2[T](lhs: Term[T], op: Operator2, rhs: Term[T]) extends Comparison[T]
  case class Comparison3[T](t1: Term[T], op: Operator3, t2: Term[T], t3: Term[T]) extends Comparison[T]
  case class And[T](e1: Expr[T], e2: Expr[T]) extends Expr[T]
  case class Or[T](e1: Expr[T], e2: Expr[T]) extends Expr[T]
  case class Not[T](e: Expr[T]) extends Expr[T]

  // Parametrized by Table type (Option[String] or Table)
  sealed trait Statement[T] {
    def tables: List[Table]
    def isQuery = false
  }

  /**
   * Returns a Statement where all columns have their tables resolved.
   */
  def resolveTables(stmt: Statement[Option[String]]): ?[Statement[Table]] = stmt match {
    case s@Select(_, _, _, _, _, _) => resolveSelect(s)()
    case d@Delete(_, _) => resolveDelete(d)()
    case u@Update(_, _, _, _, _) => resolveUpdate(u)()
    case Create() => Create[Table]().ok
    case i@Insert(_, _, _) => resolveInsert(i)()
    case s@SetStatement(_, _, _, _, _) => resolveSetStatement(s)()
    case c@Composed(_, _) => resolveComposed(c)()
  }

  private class ResolveEnv(env: List[Table]) {
    def resolve(term: Term[Option[String]]): ?[Term[Table]] = term match {
      case col@Column(_, _) => resolveColumn(col)
      case AllColumns(t) => resolveAllColumns(t)
      case f@Function(_, ps) => resolveFunc(f)
      case Subselect(select) => resolveSelect(select)(select.tables ::: env) map (s => Subselect(s))
      case ArithExpr(lhs, op, rhs) => 
        for { l <- resolve(lhs); r <- resolve(rhs) } yield ArithExpr(l, op, r)
      case c: Comparison[Option[String]] => resolveComparison(c)
      case Constant(tpe, value) => Constant[Table](tpe, value).ok
      case TermList(terms) => sequence(terms map resolve) map (ts => TermList[Table](ts))
      case Input() => Input[Table]().ok
    }

    def resolveColumn(col: Column[Option[String]]) = 
      env find { t => 
        (col.table, t.alias) match {
          case (Some(ref), None) => t.name == ref
          case (Some(ref), Some(a)) => t.name == ref || a == ref
          case (None, _) => true
        }
      } map (t => col.copy(table = t)) orFail ("Column references unknown table " + col)

    def resolveAllColumns(tableRef: Option[String]) = tableRef match {
      case Some(ref) => 
        (env.find(t => t.name == ref || t.alias.map(_ == ref).getOrElse(false)) orFail 
           ("Unknown table '" + ref + "'")) map (r => AllColumns(r))
      case None => 
        AllColumns(env.head).ok
    }

    def resolveNamed(n: Named[Option[String]]) = resolve(n.term) map (t => n.copy(term = t))
    def resolveFunc(f: Function[Option[String]]) = sequence(f.params map resolveExpr) map (ps => f.copy(params = ps))
    def resolveProj(proj: List[Named[Option[String]]]) = sequence(proj map resolveNamed)
    def resolveTableRefs(ts: List[TableReference[Option[String]]]) = sequence(ts map resolveTableRef)
    def resolveTableRef(t: TableReference[Option[String]]): ?[TableReference[Table]] = t match {
      case f@ConcreteTable(_, join) => sequence(join map resolveJoin) map (j => f.copy(join = j))
      case f@DerivedTable(_, select, join) => for {
        s <- resolveSelect(select)()
        j <- sequence(join map resolveJoin)
      } yield f.copy(subselect = s, join = j)
    }
    def resolveJoin(join: Join[Option[String]]) = for {
      t <- resolveTableRef(join.table)
      j <- sequenceO(join.joinType map resolveJoinType)
    } yield join.copy(table = t, joinType = j)
    def resolveJoinType(t: JoinType[Option[String]]): ?[JoinType[Table]] = t match {
      case QualifiedJoin(e) => resolveExpr(e) map QualifiedJoin.apply
      case NamedColumnsJoin(cs) => NamedColumnsJoin(cs).ok
    }
    def resolveWhere(where: Where[Option[String]]) = resolveExpr(where.expr) map Where.apply
    def resolveWhereOpt(where: Option[Where[Option[String]]]) = sequenceO(where map resolveWhere)
    def resolveGroupBy(groupBy: GroupBy[Option[String]]) = for {
      c <- sequence(groupBy.cols map resolveColumn)
      h <- resolveHavingOpt(groupBy.having)
    } yield groupBy.copy(cols = c, having = h)
    def resolveGroupByOpt(groupBy: Option[GroupBy[Option[String]]]) = sequenceO(groupBy map resolveGroupBy)
    def resolveHaving(having: Having[Option[String]]) = resolveExpr(having.expr) map Having.apply
    def resolveHavingOpt(having: Option[Having[Option[String]]]) = sequenceO(having map resolveHaving)
    def resolveOrderBy(orderBy: OrderBy[Option[String]]) = sequence(orderBy.sort map resolveSortKey) map (s => orderBy.copy(sort = s))
    def resolveOrderByOpt(orderBy: Option[OrderBy[Option[String]]]) = sequenceO(orderBy map resolveOrderBy)
    def resolveSortKey(k: SortKey[Option[String]]): ?[SortKey[Table]] = k match {
      case ColumnSort(c)   => resolveColumn(c) map ColumnSort.apply
      case FunctionSort(f) => resolveFunc(f) map FunctionSort.apply
      case PositionSort(p) => PositionSort(p).ok
    }
    def resolveLimit(limit: Limit[Option[String]]) = 
      Limit[Table](
        limit.count.right map (_ => Input[Table]()),
        limit.offset.map(_.right map (_ => Input[Table]()))
      ).ok
    def resolveLimitOpt(limit: Option[Limit[Option[String]]]) = sequenceO(limit map resolveLimit)

    def resolveExpr(e: Expr[Option[String]]): ?[Expr[Table]] = e match {
      case SimpleExpr(t) => resolve(t) map SimpleExpr.apply
      case c: Comparison[Option[String]] => resolveComparison(c)
      case And(e1, e2) => 
        for { r1 <- resolveExpr(e1); r2 <- resolveExpr(e2) } yield And(r1, r2)
      case Or(e1, e2) =>
        for { r1 <- resolveExpr(e1); r2 <- resolveExpr(e2) } yield Or(r1, r2)
      case Not(e) =>
        for { r <- resolveExpr(e) } yield Not(r)
    }

    def resolveComparison(c: Comparison[Option[String]]) = c match {
      case p@Comparison1(t1, op) => 
        resolve(t1) map (t => p.copy(term = t))
      case p@Comparison2(t1, op, t2) => 
        for { l <- resolve(t1); r <- resolve(t2) } yield p.copy(lhs = l, rhs = r)
      case p@Comparison3(t1, op, t2, t3) => 
        for { r1 <- resolve(t1); r2 <- resolve(t2); r3 <- resolve(t3) } yield p.copy(t1 = r1, t2 = r2, t3 = r3)
    }
  }

  private def resolveSelect(s: Select[Option[String]])(env: List[Table] = s.tables): ?[Select[Table]] = {
    val r = new ResolveEnv(env)
    for {
      p <- r.resolveProj(s.projection)
      t <- r.resolveTableRefs(s.tableReferences)
      w <- r.resolveWhereOpt(s.where) 
      g <- r.resolveGroupByOpt(s.groupBy) 
      o <- r.resolveOrderByOpt(s.orderBy)
      l <- r.resolveLimitOpt(s.limit)
    } yield s.copy(projection = p, tableReferences = t, where = w, groupBy = g, orderBy = o, limit = l)
  }

  private def resolveInsert(i: Insert[Option[String]])(env: List[Table] = i.tables): ?[Insert[Table]] = {
    val r = new ResolveEnv(env)
    (i.insertInput match {
      case SelectedInput(select) => resolveSelect(select)() map SelectedInput.apply
      case ListedInput(vals) => sequence(vals map r.resolve) map ListedInput.apply
    }) map (in => i.copy(insertInput = in))
  }

  private def resolveSetStatement(s: SetStatement[Option[String]])(env: List[Table] = s.tables): ?[SetStatement[Table]] = {
    val r = new ResolveEnv(env)
    for {
      le <- resolveTables(s.left)
      ri <- resolveTables(s.right)
      o  <- r.resolveOrderByOpt(s.orderBy)
      l  <- r.resolveLimitOpt(s.limit)
    } yield SetStatement(le, s.op, ri, o, l)
  }

  private def resolveComposed(c: Composed[Option[String]])(env: List[Table] = c.tables): ?[Composed[Table]] = {
    val r = new ResolveEnv(env)
    for {
      l <- resolveTables(c.left)
      r <- resolveTables(c.right)
    } yield Composed(l, r)
  }

  private def resolveDelete(d: Delete[Option[String]])(env: List[Table] = d.tables): ?[Delete[Table]] = {
    val r = new ResolveEnv(env)
    for {
      w <- r.resolveWhereOpt(d.where)
    } yield d.copy(where = w)
  }

  private def resolveUpdate(u: Update[Option[String]])(env: List[Table] = u.tables): ?[Update[Table]] = {
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

  case class Delete[T](tables: List[Table], where: Option[Where[T]]) extends Statement[T]

  sealed trait InsertInput[T]
  case class ListedInput[T](values: List[Term[T]]) extends InsertInput[T]
  case class SelectedInput[T](select: Select[T]) extends InsertInput[T]

  case class Insert[T](table: Table, colNames: Option[List[String]], insertInput: InsertInput[T]) extends Statement[T] {
    def output = Nil
    def tables = table :: Nil
  }

  case class SetStatement[T](left: Statement[T], op: String, right: Statement[T], 
                             orderBy: Option[OrderBy[T]], limit: Option[Limit[T]]) extends Statement[T] {
    def tables = left.tables ::: right.tables
    override def isQuery = true
  }

  case class Update[T](tables: List[Table], set: List[(Column[T], Term[T])], where: Option[Where[T]], 
                       orderBy: Option[OrderBy[T]], limit: Option[Limit[T]]) extends Statement[T]

  case class Create[T]() extends Statement[T] {
    def tables = Nil
  }

  case class Composed[T](left: Statement[T], right: Statement[T]) extends Statement[T] {
    def tables = left.tables ::: right.tables
    override def isQuery = left.isQuery || right.isQuery
  }

  case class Select[T](projection: List[Named[T]], 
                       tableReferences: List[TableReference[T]], // should be NonEmptyList
                       where: Option[Where[T]], 
                       groupBy: Option[GroupBy[T]],
                       orderBy: Option[OrderBy[T]],
                       limit: Option[Limit[T]]) extends Statement[T] {
    def tables = tableReferences flatMap (_.tables)
    override def isQuery = true
  }

  trait TableReference[T] {
    def tables: List[Table]
  }
  case class ConcreteTable[T](table: Table, join: List[Join[T]]) extends TableReference[T] {
    def tables = table :: join.flatMap(_.table.tables)
  }
  case class DerivedTable[T](name: String, subselect: Select[T], join: List[Join[T]]) extends TableReference[T] {
    def tables = Table(name, None) :: join.flatMap(_.table.tables)
  }

  case class Where[T](expr: Expr[T])

  case class Join[T](table: TableReference[T], joinType: Option[JoinType[T]], joinDesc: String)

  trait JoinType[T]
  case class QualifiedJoin[T](expr: Expr[T]) extends JoinType[T]
  case class NamedColumnsJoin[T](columns: List[String]) extends JoinType[T]

  case class GroupBy[T](cols: List[Column[T]], having: Option[Having[T]])

  case class Having[T](expr: Expr[T])

  case class OrderBy[T](sort: List[SortKey[T]], orders: List[Option[Order]])

  sealed trait SortKey[T]
  case class ColumnSort[T](col: Column[T]) extends SortKey[T]
  case class FunctionSort[T](f: Function[T]) extends SortKey[T]
  case class PositionSort[T](pos: Int) extends SortKey[T]

  sealed trait Order
  case object Asc extends Order
  case object Desc extends Order

  case class Limit[T](count: Either[Int, Input[T]], offset: Option[Either[Int, Input[T]]])
}

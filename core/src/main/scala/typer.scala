package sqltyped

import schemacrawler.schema.{ColumnDataType, Schema}
import scala.reflect.runtime.universe.{Type, typeOf}
import Ast._

case class TypedValue(name: String, tpe: (Type, Int), nullable: Boolean, tag: Option[String], term: Term[Table])

case class TypedStatement(
    input: List[TypedValue]
  , output: List[TypedValue]
  , isQuery: Boolean
  , uniqueConstraints: Map[Table, List[List[Column[Table]]]]
  , generatedKeyTypes: List[TypedValue]
  , numOfResults: NumOfResults.NumOfResults = NumOfResults.Many)

object NumOfResults extends Enumeration {
  type NumOfResults = Value
  val ZeroOrOne, One, Many = Value
}

/**
 * Variable is a placeholder in SQL statement (ie. ?-char).
 * 'comparisonTerm' is the outer context of a variable. For instance in the stmt below:
 *     where name = upper(?)
 * the comparison term of Function("upper") is Comparison2(Column("name"), Eq, f)
 */
case class Variable(term: Named[Table], comparisonTerm: Option[Term[Table]] = None)

class Variables(typer: Typer) extends Ast.Resolved {
  def input(schema: Schema, stmt: Statement): List[Variable] = stmt match {
    case Delete(from, where) => where map (w => input(w.expr, None)) getOrElse Nil

    case Insert(table, colNames, insertInput) =>
      def colNamesFromSchema = schema.getTable(table.name).getColumns.toList.map(_.getName)

      insertInput match {
        case ListedInput(values) => 
          (colNames getOrElse colNamesFromSchema zip values collect {
            case (name, Input()) => List(Variable(Named(name, None, Column(name, table))))
            case (name, Subselect(s)) => input(schema, s)
          }).flatten
        case SelectedInput(select) => input(schema, select)
      }

    case SetStatement(l, op, r, orderBy, limit) => 
      input(schema, l) ::: input(schema, r) ::: (orderBy map input).getOrElse(Nil) ::: limitInput(limit)

    case Composed(left, right) => 
      input(schema, left) ::: input(schema, right)

    case Update(tables, set, where, orderBy, limit) => 
      set.flatMap { 
        case (col, Input()) => Variable(Named(col.name, None, col)) :: Nil
        case (col, t) => inputTerm(t, None)
      } :::
      where.map(w => input(w.expr, None)).getOrElse(Nil) ::: 
      (orderBy map input).getOrElse(Nil) :::
      limitInput(limit)

    case Create() => Nil

    case s@Select(_, _, _, _, _, _) => input(s)
  }

  def input(s: Select): List[Variable] =
    s.projection.collect { 
      case Named(n, a, f@Function(_, _)) => input(f, None) 
      case n@Named(_, _, Input())        => Variable(n) :: Nil
      case Named(_, _, Subselect(s))     => input(s)
      case Named(_, _, e: Expr)          => input(e, None)
      case Named(_, _, c@Case(_, _))     => inputTerm(c, None)
    }.flatten :::
    s.tableReferences.flatMap(input) :::
    s.where.map(w => input(w.expr, None)).getOrElse(Nil) ::: 
    s.groupBy.toList.flatMap(g => (g.terms flatMap (t => inputTerm(t, None))) ::: g.having.toList.flatMap(h => input(h.expr, None))) :::
    s.orderBy.map(input).getOrElse(Nil) :::
    limitInput(s.limit)

  def input(t: TableReference): List[Variable] = t match {
    case ConcreteTable(_, join) => join flatMap input
    case DerivedTable(_, s, join) => input(s) ::: (join flatMap input)
  }

  def input(j: Join): List[Variable] =
    input(j.table) ::: (j.joinType map input getOrElse Nil)

  def input(o: OrderBy): List[Variable] = (o.sort collect {
    case FunctionSort(f) => input(f, None)
    case VariablePositionSort() => Variable(Named("orderby", None, Input[Table]())) :: Nil
  }).flatten

  def input(j: JoinType): List[Variable] = j match {
    case QualifiedJoin(e) => input(e, None)
    case _ => Nil
  }

  def input(f: Function, comparisonTerm: Option[Term]): List[Variable] = 
    f.params zip typer.inferArguments(f, comparisonTerm).getOrElse(Nil) flatMap {
      case (SimpleExpr(t), (tpe, _)) => t match {
        case Input() => Variable(Named("<farg>", None, Constant[Table](tpe, ())), comparisonTerm) :: Nil
        case _ => inputTerm(t, comparisonTerm)
      }
      case (e, _) => input(e, comparisonTerm)
    }

  def nameVar(t: Term, comparisonTerm: Option[Term]) = t match {
    case c@Constant(_, _) => Variable(Named("<constant>", None, c), comparisonTerm)
    case f@Function(n, _) => Variable(Named(n, None, f), comparisonTerm)
    case c@Column(n, _)   => Variable(Named(n, None, c), comparisonTerm)
    case c@AllColumns(_)  => Variable(Named("*", None, c), comparisonTerm)
    case Subselect(s)     => Variable(s.projection.head, comparisonTerm)
    case _ => sys.error("Invalid term " + t)
  }

  def inputTerm(t: Term, comparisonTerm: Option[Term]): List[Variable] = t match {
    case f@Function(_, _)         => input(f, comparisonTerm)
    case Subselect(s)             => input(s)
    case ArithExpr(Input(), _, t) => nameVar(t, comparisonTerm) :: inputTerm(t, comparisonTerm)
    case ArithExpr(t, _, Input()) => inputTerm(t, comparisonTerm) ::: List(nameVar(t, comparisonTerm))
    case ArithExpr(lhs, _, rhs)   => inputTerm(lhs, comparisonTerm) ::: inputTerm(rhs, comparisonTerm)
    case Case(conds, elze)        => (conds flatMap { case (expr, result) => 
                                       input(expr, comparisonTerm) ::: inputTerm(result, comparisonTerm)
                                     }) ::: (elze map (t => inputTerm(t, comparisonTerm)) getOrElse Nil) 
    case _ => Nil
  }

  object Inputs {
    def unapply(t: Term) = t match {
      case TermList(ts) => Some(ts collect { case Input() => Input() })
      case _ => None
    }
  }

  def input(e: Expr, ct: Option[Term]): List[Variable] = e match {
    case SimpleExpr(t)                       => inputTerm(t, ct)
    case c@Comparison1(t, _)                 => inputTerm(t, Some(c))
    case c@Comparison2(Input(), op, t)       => nameVar(t, Some(c)) :: inputTerm(t, Some(c))
    case c@Comparison2(t, op, Input())       => inputTerm(t, Some(c)) ::: List(nameVar(t, Some(c)))
    case c@Comparison2(Inputs(is), op, t)    => (is map (_ => nameVar(t, Some(c)))) ::: inputTerm(t, Some(c))
    case c@Comparison2(t, op, Inputs(is))    => inputTerm(t, Some(c)) ::: (is map (_ => nameVar(t, Some(c))))
    case c@Comparison2(t, op, Subselect(s))  => inputTerm(t, Some(c)) ::: input(s)
    case c@Comparison2(t1, op, t2)           => inputTerm(t1, Some(c)) ::: inputTerm(t2, Some(c))
    case c@Comparison3(t, op, Input(), Input()) => inputTerm(t, Some(c)) ::: (nameVar(t, Some(c)) :: nameVar(t, Some(c)) :: Nil)
    case c@Comparison3(t1, op, Input(), t2)  => inputTerm(t1, Some(c)) ::: (nameVar(t1, Some(c)) :: inputTerm(t2, Some(c)))
    case c@Comparison3(t1, op, t2, Input())  => inputTerm(t1, Some(c)) ::: inputTerm(t2, Some(c)) ::: List(nameVar(t1, Some(c)))
    case c@Comparison3(t1, op, t2, t3)       => inputTerm(t1, Some(c)) ::: inputTerm(t2, Some(c)) ::: inputTerm(t3, Some(c))
    case And(e1, e2)                         => input(e1, ct) ::: input(e2, ct)
    case Or(e1, e2)                          => input(e1, ct) ::: input(e2, ct)
    case Not(e)                              => input(e, ct)
    case TypeExpr(d)                         => Nil
  }

  def limitInput(limit: Option[Limit]) =
    limit.map(l => l.count.right.toSeq.toList ::: l.offset.map(_.right.toSeq.toList).getOrElse(Nil)).getOrElse(Nil).map { _ => 
      Variable(Named("<constant>", None, Constant[Table]((typeOf[Long], java.sql.Types.BIGINT), None)))
    }

  def output(stmt: Statement): List[Variable] = stmt match {
    case Delete(_, _) => Nil
    case Insert(_, _, _) => Nil
    case SetStatement(left, _, _, _, _) => output(left)
    case Composed(left, right) => output(left) ::: output(right)
    case Update(_, _, _, _, _) => Nil
    case Create() => Nil
    case Select(projection, _, _, _, _, _) => projection map (t => Variable(t))
  }
}

class Typer(schema: Schema, stmt: Ast.Statement[Table]) extends Ast.Resolved {
  import java.sql.{Types => JdbcTypes}

  type SqlType = ((Type, Int), Boolean)
  type SqlFType = (List[SqlType], SqlType)

  def typeSpecifyTerm(v: Variable): Option[?[List[TypedValue]]] = None

  def infer(useInputTags: Boolean): ?[TypedStatement] = {
    def uniqueConstraints = {
      val constraints = stmt.tables map { t =>
        (tableSchema(t) map { table =>
          val indices = Option(table.getPrimaryKey).map(List(_)).getOrElse(Nil) ::: table.getIndices.toList
          val uniques = indices filter (_.isUnique) map { i =>
            i.getColumns.toList.map(col => Column(col.getName, t))
          }
          (t, uniques)
        }) fold (_ => (t, List[List[Column]]()), identity)
      }

      Map[Table, List[List[Column]]]().withDefault(_ => Nil) ++ constraints
    }

    def generatedKeyTypes(table: Table) = (for {
      t <- tableSchema(table)
    } yield {
      def tag(c: schemacrawler.schema.Column) = 
        Option(t.getPrimaryKey).flatMap(_.getColumns.find(_.getName == c.getName)).map(_ => t.getName)

      t.getColumns.toList
        .filter(c => c.getType.isAutoIncrementable)
        .map(c => TypedValue(c.getName, mkType(c.getType), false, tag(c), Column(c.getName, table)))
    }) fold (_ => Nil, identity)

    val vars = new Variables(this)
    for {
      in  <- sequence(vars.input(schema, stmt) map typeTerm(useTags = useInputTags))
      out <- sequence(vars.output(stmt) map typeTerm(useTags = true))
    } yield TypedStatement(in.flatten, out.flatten, stmt.isQuery, uniqueConstraints, generatedKeyTypes(stmt.tables.head))
  }

  def tag(col: Column) =
    tableSchema(col.table) map { t =>
      def findFK = t.getForeignKeys
        .flatMap(_.getColumnPairs.map(_.getForeignKeyColumn))
        .find(_.getName == col.name)
        .map(_.getReferencedColumn.getParent.getName)

      if (t.getPrimaryKey != null && t.getPrimaryKey.getColumns.exists(_.getName == col.name))
        Some(col.table.name)
      else findFK orElse None
    } fold (_ => None, identity)

  def typeTerm(useTags: Boolean)(v: Variable): ?[List[TypedValue]] = typeSpecifyTerm(v) getOrElse { 
    val x = v.term
    x.term match {
      case col@Column(_, _) => 
        for {
          (tpe, opt) <- inferColumnType(col)
        } yield List(TypedValue(x.aname, tpe, opt, if (useTags) tag(col) else None, x.term))
      case AllColumns(t) =>
        for {
          tbl <- tableSchema(t)
          cs  <- sequence(tbl.getColumns.toList map { c => typeTerm(useTags)(Variable(Named(c.getName, None, Column(c.getName, t)))) })
        } yield cs.flatten
      case f@Function(_, _) =>
        inferReturnType(f, v.comparisonTerm) map { case (tpe, opt) =>
          List(TypedValue(x.aname, tpe, opt, None, x.term))
        }
      case Constant(tpe, _) => List(TypedValue(x.aname, tpe, false, None, x.term)).ok
      case Input() => 
        List(TypedValue(x.aname, (typeOf[Any], JdbcTypes.JAVA_OBJECT), false, None, x.term)).ok
      case ArithExpr(_, "/", _) => 
        List(TypedValue(x.aname, (typeOf[Double], JdbcTypes.DOUBLE), true, None, x.term)).ok
      case ArithExpr(lhs, _, rhs) => 
        (lhs, rhs) match {
          case (c@Column(_, _), _) => typeTerm(useTags)(Variable(Named(c.name, x.alias, c), v.comparisonTerm))
          case (_, c@Column(_, _)) => typeTerm(useTags)(Variable(Named(c.name, x.alias, c), v.comparisonTerm))
          case _ => typeTerm(useTags)(Variable(Named(x.name, x.alias, lhs), v.comparisonTerm))
        }
      case Comparison1(_, IsNull) | Comparison1(_, IsNotNull) => 
        List(TypedValue(x.aname, (typeOf[Boolean], JdbcTypes.BOOLEAN), false, None, x.term)).ok
      case Comparison1(t, _) => 
        List(TypedValue(x.aname, (typeOf[Boolean], JdbcTypes.BOOLEAN), isNullable(t), None, x.term)).ok
      case Comparison2(t1, _, t2) => 
        List(TypedValue(x.aname, (typeOf[Boolean], JdbcTypes.BOOLEAN), isNullable(t1) || isNullable(t2), None, x.term)).ok
      case Comparison3(t1, _, t2, t3) => 
        List(TypedValue(x.aname, (typeOf[Boolean], JdbcTypes.BOOLEAN), isNullable(t1) || isNullable(t2) || isNullable(t3), None, x.term)).ok
      case Subselect(s) => 
        sequence(s.projection map (t => Variable(t)) map typeTerm(useTags)) map (_.flatten) map (_ map makeNullable)
      case TermList(t) => 
        sequence(t.map(t => typeTerm(useTags)(Variable(Named("elem", None, t), v.comparisonTerm)))).map(_.flatten)
      case Case(conds, elze) =>
        typeTerm(useTags)(Variable(Named("case", None, conds.head._2), v.comparisonTerm))
    }
  }

  def makeNullable(x: TypedValue) = x.copy(nullable = true)

  def isNullable(t: Term) = tpeOf(SimpleExpr(t), None) map { case (_, opt) => opt } getOrElse false

  def isAggregate(fname: String): Boolean = aggregateFunctions.contains(fname.toLowerCase)

  val dsl = new TypeSigDSL(this)
  import dsl._

  val aggregateFunctions = Map(
      "avg"   -> (f(a) -> option(double))
    , "count" -> (f(a) -> long)
    , "min"   -> (f(a) -> a)
    , "max"   -> (f(a) -> a)
    , "sum"   -> (f(a) -> a)
  ) ++ extraAggregateFunctions

  val scalarFunctions = Map(
      "abs"   -> (f(a) -> a)
    , "lower" -> (f(a) -> a)
    , "upper" -> (f(a) -> a)
    , "|"     -> (f2(a, a) -> a)
    , "&"     -> (f2(a, a) -> a)
    , "^"     -> (f2(a, a) -> a)
    , ">>"    -> (f2(a, a) -> a)
    , "<<"    -> (f2(a, a) -> a)
  ) ++ extraScalarFunctions

  val knownFunctions = aggregateFunctions ++ scalarFunctions

  def extraAggregateFunctions: Map[String, (String, List[Expr], Option[Term]) => ?[SqlFType]] = Map()
  def extraScalarFunctions: Map[String, (String, List[Expr], Option[Term]) => ?[SqlFType]] = Map()
  
  def tpeOf(e: Expr, comparisonTerm: Option[Term]): ?[SqlType] = e match {
    case SimpleExpr(t) => t match {
      case Constant(tpe, x) if x == null => (tpe, true).ok
      case Constant(tpe, _)              => (tpe, false).ok
      case col@Column(_, _)              => inferColumnType(col)
      case f@Function(_, _)              => inferReturnType(f, comparisonTerm)
      case Input() => (comparisonTerm map typeFromComparison) getOrElse ((typeOf[Any], JdbcTypes.JAVA_OBJECT), false).ok
      case TermList(terms)               => tpeOf(SimpleExpr(terms.head), comparisonTerm)
      case ArithExpr(Input(), op, rhs)   => tpeOf(SimpleExpr(rhs), comparisonTerm)
      case ArithExpr(lhs, op, rhs)       => tpeOf(SimpleExpr(lhs), comparisonTerm)
      case x                             => sys.error("Term " + x + " not supported")
    }

    case _                               => ((typeOf[Boolean], JdbcTypes.BOOLEAN), false).ok
  }

  def typeFromComparison(term: Term) = term match {
    case Comparison2(t, _, _) => typeTerm(false)(Variable(Named("", None, t))) map (ts => (ts.head.tpe, ts.head.nullable))
    case x => ((typeOf[Any], JdbcTypes.JAVA_OBJECT), false).ok
  }

  def inferReturnType(f: Function, comparisonTerm: Option[Term]) = 
    knownFunctions.get(f.name.toLowerCase) match {
      case Some(func) => func(f.name, f.params, comparisonTerm).map(_._2)
      case None => ((typeOf[Any], JdbcTypes.JAVA_OBJECT), true).ok
    }

  def inferArguments(f: Function, comparisonTerm: Option[Term]) = 
    knownFunctions.get(f.name.toLowerCase) match {
      case Some(func) => func(f.name, f.params, comparisonTerm).map(_._1)
      case None => f.params.map(_ => ((typeOf[Any], JdbcTypes.JAVA_OBJECT), true)).ok
    }

  def inferColumnType(col: Column) = (for {
    t <- tableSchema(col.table)
    c <- Option(t.getColumn(col.name)) orFail ("No such column " + col)
  } yield (mkType(c.getType), c.isNullable || isNullableByJoin(col) || isNullableByGroupBy(col))) fold (
    _ => inferFromDerivedTable(col), x => x.ok
  )

  def inferFromDerivedTable(col: Column) = for {
    t <- derivedTable(col.table)
    c <- t.output.find(_.name == col.name) orFail ("No such column " + col)
  } yield (c.tpe, c.nullable)

  def isNullableByJoin(col: Column) = isProjectedByJoin(stmt, col) map (_.joinDesc) exists {
    case LeftOuter | RightOuter | FullOuter => true
    case Inner | Cross => false
  }

  def isNullableByGroupBy(col: Column) = stmt match {
    case Select(_, _, _, Some(GroupBy(terms, true, _)), _, _) => terms.contains(col)
    case _ => false
  }

  private def tableSchema(tbl: Table) =
    if (tbl.name.toLowerCase == "dual") DualTable(schema).ok
    else Option(schema.getTable(tbl.name)) orFail ("Unknown table " + tbl.name)

  private def derivedTable(tbl: Table) = for {
    t     <- DerivedTables(schema, stmt, tbl.name) orFail ("Unknown table XXX " + tbl.name)
    typed <- new Typer(schema, t).infer(false)
  } yield typed

  private def mkType(t: ColumnDataType) = (Jdbc.mkType(t.getTypeClassName), t.getType)
}

object DualTable {
  def apply(schema: Schema) = {
    val cstr = schema.getClass.getClassLoader.loadClass("schemacrawler.crawl.MutableTable")
      .getDeclaredConstructor(classOf[Schema], classOf[String])
    cstr.setAccessible(true)
    cstr.newInstance(schema, "dual").asInstanceOf[schemacrawler.schema.Table]
  }
}

object DerivedTables extends Ast.Resolved {
  def apply(schema: Schema, stmt: Statement, name: String): Option[Select] = 
    derivedTables(stmt) find (_.name == name) map (_.subselect)

  private def derivedTables(stmt: Statement): List[DerivedTable] = stmt match {
    case Select(proj, tableRefs, where, groupBy, orderBy, limit) => tableRefs flatMap referencedTables
    case _ => Nil
  }

  private def joinedTables(j: Join) = referencedTables(j.table)

  private def referencedTables(table: TableReference): List[DerivedTable] = table match {
    case ConcreteTable(_, join) => join flatMap joinedTables
    case t@DerivedTable(_, sub, join) => t :: derivedTables(sub) ::: (join flatMap joinedTables)
  }
}

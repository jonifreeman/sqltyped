package sqltyped

import schemacrawler.schema.{ColumnDataType}
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.{Type, typeOf}
import Ast._

case class TypedValue(name: String, tpe: Type, nullable: Boolean, tag: Option[String], term: Term[Table])

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

class Variables(typer: Typer) extends Ast.Resolved {
  def input(schema: DbSchema, stmt: Statement): List[Named] = stmt match {
    case Delete(from, where) => where.map(w => input(w.expr)).getOrElse(Nil)

    case Insert(table, colNames, insertInput) =>
      def colNamesFromSchema = schema.getTable(table.name).getColumns.asScala.toList.map(_.getName)

      insertInput match {
        case ListedInput(values) => 
          (colNames getOrElse colNamesFromSchema zip values collect {
            case (name, Input()) => List(Named(name, None, Column(name, table)))
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
        case (col, Input()) => Named(col.name, None, col) :: Nil
        case (col, t) => inputTerm(t)
      } :::
      where.map(w => input(w.expr)).getOrElse(Nil) ::: 
      (orderBy map input).getOrElse(Nil) :::
      limitInput(limit)

    case Create() => Nil

    case s@Select(_, _, _, _, _, _) => input(s)
  }

  def input(s: Select): List[Named] =
    s.projection.collect { 
      case Named(n, a, f@Function(_, _)) => input(f) 
      case n@Named(_, _, Input())        => n :: Nil
      case Named(_, _, Subselect(s))     => input(s)
      case Named(_, _, e: Expr)          => input(e)
    }.flatten :::
    s.tableReferences.flatMap(input) :::
    s.where.map(w => input(w.expr)).getOrElse(Nil) ::: 
    s.groupBy.flatMap(g => g.having.map(h => input(h.expr))).getOrElse(Nil) :::
    s.orderBy.map(input).getOrElse(Nil) :::
    limitInput(s.limit)

  def input(t: TableReference): List[Named] = t match {
    case ConcreteTable(_, join) => join flatMap input
    case DerivedTable(_, s, join) => input(s) ::: (join flatMap input)
  }

  def input(j: Join): List[Named] =
    input(j.table) ::: (j.joinType map input getOrElse Nil)

  def input(o: OrderBy): List[Named] = (o.sort collect {
    case FunctionSort(f) => input(f)
    case VariablePositionSort() => Named("orderby", None, Input[Table]()) :: Nil
  }).flatten

  def input(j: JoinType): List[Named] = j match {
    case QualifiedJoin(e) => input(e)
    case _ => Nil
  }

  def input(f: Function): List[Named] = f.params zip typer.inferArguments(f).getOrElse(Nil) flatMap {
    case (SimpleExpr(t), (tpe, _)) => t match {
      case Input() => Named("<farg>", None, Constant[Table](tpe, ())) :: Nil
      case _ => inputTerm(t)
    }
    case (e, _) => input(e)
  }

  def nameTerm(t: Term) = t match {
    case c@Constant(_, _) => Named("<constant>", None, c)
    case f@Function(n, _) => Named(n, None, f)
    case c@Column(n, _)   => Named(n, None, c)
    case c@AllColumns(_)  => Named("*", None, c)
    case Subselect(s)     => s.projection.head
    case _ => sys.error("Invalid term " + t)
  }

  def inputTerm(t: Term): List[Named] = t match {
    case f@Function(_, _)         => input(f)
    case Subselect(s)             => input(s)
    case ArithExpr(Input(), _, t) => nameTerm(t) :: inputTerm(t)
    case ArithExpr(t, _, Input()) => inputTerm(t) ::: List(nameTerm(t))
    case ArithExpr(lhs, _, rhs)   => inputTerm(lhs) ::: inputTerm(rhs)
    case _ => Nil
  }

  def input(e: Expr): List[Named] = e match {
    case SimpleExpr(t)                        => inputTerm(t)
    case Comparison1(t, _)                    => inputTerm(t)
    case Comparison2(Input(), op, t)          => nameTerm(t) :: inputTerm(t)
    case Comparison2(t, op, Input())          => inputTerm(t) ::: List(nameTerm(t))
    case Comparison2(t, op, Subselect(s))     => inputTerm(t) ::: input(s)
    case Comparison2(t1, op, t2)              => inputTerm(t1) ::: inputTerm(t2)
    case Comparison3(t, op, Input(), Input()) => inputTerm(t) ::: (nameTerm(t) :: nameTerm(t) :: Nil)
    case Comparison3(t1, op, Input(), t2)     => inputTerm(t1) ::: (nameTerm(t1) :: inputTerm(t2))
    case Comparison3(t1, op, t2, Input())     => inputTerm(t1) ::: inputTerm(t2) ::: List(nameTerm(t1))
    case Comparison3(t1, op, t2, t3)          => inputTerm(t1) ::: inputTerm(t2) ::: inputTerm(t3)
    case And(e1, e2)                          => input(e1) ::: input(e2)
    case Or(e1, e2)                           => input(e1) ::: input(e2)
    case Not(e)                               => input(e)
    case TypeExpr(d)                          => Nil
  }

  def limitInput(limit: Option[Limit]) =
    limit.map(l => l.count.right.toSeq.toList ::: l.offset.map(_.right.toSeq.toList).getOrElse(Nil))
      .getOrElse(Nil).map(_ => Named("<constant>", None, Constant[Table](typeOf[Long], None)))

  def output(stmt: Statement): List[Named] = stmt match {
    case Delete(_, _) => Nil
    case Insert(_, _, _) => Nil
    case SetStatement(left, _, _, _, _) => output(left)
    case Composed(left, right) => output(left) ::: output(right)
    case Update(_, _, _, _, _) => Nil
    case Create() => Nil
    case Select(projection, _, _, _, _, _) => projection
  }
}

class Typer(schema: DbSchema, stmt: Ast.Statement[Table]) extends Ast.Resolved {
  type SqlType = (Type, Boolean)
  type SqlFType = (List[SqlType], SqlType)

  def infer(useInputTags: Boolean): ?[TypedStatement] = {
    def tag(col: Column) =
      tableSchema(col.table) map { t =>
        def findFK = t.getForeignKeys.asScala
          .flatMap(_.getColumnReferences.asScala.map(_.getForeignKeyColumn))
          .find(_.getName == col.name)
          .map(_.getReferencedColumn.getParent.getName)

        if (t.getPrimaryKey != null && t.getPrimaryKey.getColumns.asScala.exists(_.getName == col.name))
          Some(col.table.name)
        else findFK orElse None
      }

    def typeTerm(useTags: Boolean)(x: Named): ?[List[TypedValue]] = x.term match {
      case col@Column(_, _) => 
        for {
          (tpe, opt) <- inferColumnType(col)
          t <- tag(col)
        } yield List(TypedValue(x.aname, tpe, opt, if (useTags) t else None, x.term))
      case AllColumns(t) =>
        for {
          tbl <- tableSchema(t)
          cs  <- sequence(tbl.getColumns.asScala.toList map { c => typeTerm(useTags)(Named(c.getName, None, Column(c.getName, t))) })
        } yield cs.flatten
      case f@Function(_, _) =>
        inferReturnType(f) map { case (tpe, opt) =>
          List(TypedValue(x.aname, tpe, opt, None, x.term))
        }
      case Constant(tpe, _) => List(TypedValue(x.aname, tpe, false, None, x.term)).ok
      case Input() => List(TypedValue(x.aname, typeOf[Any], false, None, x.term)).ok
      case ArithExpr(_, "/", _) => List(TypedValue(x.aname, typeOf[Double], true, None, x.term)).ok
      case ArithExpr(lhs, _, rhs) => 
        (lhs, rhs) match {
          case (c@Column(_, _), _) => typeTerm(useTags)(Named(c.name, x.alias, c))
          case (_, c@Column(_, _)) => typeTerm(useTags)(Named(c.name, x.alias, c))
          case _ => typeTerm(useTags)(Named(x.name, x.alias, lhs))
        }
      case Comparison1(_, IsNull) | Comparison1(_, IsNotNull) => 
        List(TypedValue(x.aname, typeOf[Boolean], false, None, x.term)).ok
      case Comparison1(t, _) => 
        List(TypedValue(x.aname, typeOf[Boolean], isNullable(t), None, x.term)).ok
      case Comparison2(t1, _, t2) => 
        List(TypedValue(x.aname, typeOf[Boolean], isNullable(t1) || isNullable(t2), None, x.term)).ok
      case Comparison3(t1, _, t2, t3) => 
        List(TypedValue(x.aname, typeOf[Boolean], isNullable(t1) || isNullable(t2) || isNullable(t3), None, x.term)).ok
      case Subselect(s) => 
        sequence(s.projection map typeTerm(useTags)) map (_.flatten) map (_ map makeNullable)
      case TermList(t) => 
        sequence(t.map(t => typeTerm(useTags)(Named("elem", None, t)))).map(_.flatten)
    }

    def makeNullable(x: TypedValue) = x.copy(nullable = true)

    def isNullable(t: Term) = tpeOf(SimpleExpr(t)) map { case (_, opt) => opt } getOrElse false

    def uniqueConstraints = {
      val constraints = sequence(stmt.tables map { t =>
        tableSchema(t) map { table =>
          val indices = Option(table.getPrimaryKey).toList ::: table.getIndices.asScala.toList
          val uniques = indices filter (_.isUnique) map { i =>
            i.getColumns.asScala.toList.map(col => Column(col.getName, t))
          }
          (t, uniques)
        }
      })

      constraints map (cs => Map[Table, List[List[Column]]]().withDefault(_ => Nil) ++ cs)
    }

    def generatedKeyTypes(table: Table) = for {
      t <- tableSchema(table)
    } yield {
      def tag(c: schemacrawler.schema.Column) = 
        Option(t.getPrimaryKey).flatMap(_.getColumns.asScala.find(_.getName == c.getName)).map(_ => t.getName)

      t.getColumns.asScala.toList
        .filter(c => c.getColumnDataType.isAutoIncrementable)
        .map(c => TypedValue(c.getName, mkType(c.getColumnDataType), false, tag(c), Column(c.getName, table)))
    }

    val vars = new Variables(this)
    for {
      in  <- sequence(vars.input(schema, stmt) map typeTerm(useTags = useInputTags))
      out <- sequence(vars.output(stmt) map typeTerm(useTags = true))
      ucs <- uniqueConstraints
      key <- generatedKeyTypes(stmt.tables.head)
    } yield TypedStatement(in.flatten, out.flatten, stmt.isQuery, ucs, key)
  }

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

  def extraAggregateFunctions: Map[String, (String, List[Expr]) => ?[SqlFType]] = Map()
  def extraScalarFunctions: Map[String, (String, List[Expr]) => ?[SqlFType]] = Map()
  
  def tpeOf(e: Expr): ?[SqlType] = e match {
    case SimpleExpr(t) => t match {
      case Constant(tpe, x) if x == null => (tpe, true).ok
      case Constant(tpe, _)              => (tpe, false).ok
      case col@Column(_, _)              => inferColumnType(col)
      case f@Function(_, _)              => inferReturnType(f)
      case Input()                       => (typeOf[Any], false).ok
      case TermList(terms)               => tpeOf(SimpleExpr(terms.head))
      case ArithExpr(Input(), op, rhs)   => tpeOf(SimpleExpr(rhs))
      case ArithExpr(lhs, op, rhs)       => tpeOf(SimpleExpr(lhs))
      case x                             => sys.error("Term " + x + " not supported")
    }

    case _                               => (typeOf[Boolean], false).ok
  }

  def inferReturnType(f: Function) = 
    knownFunctions.get(f.name.toLowerCase) match {
      case Some(func) => func(f.name, f.params).map(_._2)
      case None => (typeOf[Any], true).ok
    }

  def inferArguments(f: Function) = 
    knownFunctions.get(f.name.toLowerCase) match {
      case Some(func) => func(f.name, f.params).map(_._1)
      case None => f.params.map(_ => (typeOf[Any], true)).ok
    }

  def inferColumnType(col: Column) = for {
    t <- tableSchema(col.table)
    c <- Option(t.getColumn(col.name)) orFail ("No such column " + col)
  } yield (mkType(c.getColumnDataType), c.isNullable || isNullableByJoin(col))

  def isNullableByJoin(col: Column) = isProjectedByJoin(stmt, col) map (_.joinDesc) exists {
    case LeftOuter | RightOuter | FullOuter => true
    case Inner | Cross => false
  }

  private def tableSchema(t: Table) =
    if (t.name.toLowerCase == "dual") DualTable(schema).ok
    else Option(schema.getTable(t.name)) orElse derivedTable(t.name) orFail ("Unknown table " + t.name)

  private def derivedTable(name: String) = DerivedTables(schema, stmt, name)
  private def mkType(t: ColumnDataType) = Jdbc.mkType(t.getTypeClassName)
}

object DualTable {
  def apply(schema: DbSchema) = {
    val cstr = schema.getClass.getClassLoader.loadClass("schemacrawler.crawl.MutableTable")
      .getDeclaredConstructor(classOf[DbSchema], classOf[String])
    cstr.setAccessible(true)
    cstr.newInstance(schema, "dual").asInstanceOf[schemacrawler.schema.Table]
  }
}

object DerivedTables extends Ast.Resolved {
  def apply(schema: DbSchema, stmt: Statement, name: String): Option[schemacrawler.schema.Table] = 
    derivedTables(stmt) find (_.name == name) map mkTable(schema)

  private def derivedTables(stmt: Statement): List[DerivedTable] = stmt match {
    case Select(proj, tableRefs, where, groupBy, orderBy, limit) => tableRefs flatMap referencedTables
    case _ => Nil
  }

  private def joinedTables(j: Join) = referencedTables(j.table)

  private def referencedTables(table: TableReference): List[DerivedTable] = table match {
    case ConcreteTable(_, join) => join flatMap joinedTables
    case t@DerivedTable(_, sub, join) => t :: derivedTables(sub) ::: (join flatMap joinedTables)
  }

  private def mkTable(schema: DbSchema)(t: DerivedTable) = new schemacrawler.schema.Table {
    def getColumn(name: String) = 
      t.subselect.projection find(_.name == name) flatMap mkColumn getOrElse(null)

    private def columns = t.subselect.projection flatMap mkColumn
    def getColumns = columns.asJava

    private def mkColumn(n: Named) = n.term match {
      case Column(name, table) => Option(schema.getTable(table.name).getColumn(name))
      case _ => None
    }

    def getAttribute(name: String) = ???
    def getAttribute[T](name: String, defaultValue: T) = ???
    def getAttributes = ???
    def getFullName = t.name
    def getName = t.name
    def getLookupKey = ???
    def getRemarks = ""
    def setAttribute(name: String, value: AnyRef) = ???
    def getSchema = schema.schema
    def getCheckConstraints = java.util.Collections.emptyList()
    def getColumnsListAsString = columns.mkString(",")
    def getExportedForeignKeys = java.util.Collections.emptyList()
    def getForeignKey(name: String) = null
    def getForeignKeys = java.util.Collections.emptyList()
    def getImportedForeignKeys = java.util.Collections.emptyList()
    def getIndex(name: String) = null
    def getIndices = java.util.Collections.emptyList()
    def getPrimaryKey = null
    def getPrivilege(name: String) = ???
    def getPrivileges = java.util.Collections.emptyList()
    def getRelatedTables(t: schemacrawler.schema.TableRelationshipType) = java.util.Collections.emptyList()
    def getTrigger(name: String) = null
    def getTriggers = java.util.Collections.emptyList()
    def getTableType = ???
    def getType = ???
    def compareTo(n: schemacrawler.schema.NamedObject) = getName compareTo n.getName
  }
}

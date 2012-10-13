package sqltyped

import schemacrawler.schemacrawler._
import schemacrawler.schema.{ColumnDataType, Schema}
import schemacrawler.utility.SchemaCrawlerUtility
import scala.reflect.runtime.universe.{Type, typeOf}
import Ast._

case class TypedValue(name: String, tpe: Type, nullable: Boolean, tag: Option[String])

case class TypedStatement(
    input: List[TypedValue]
  , output: List[TypedValue]
  , stmt: Statement[Table]
  , uniqueConstraints: Map[Table, List[List[Column[Table]]]]
  , generatedKeyTypes: List[TypedValue]
  , numOfResults: NumOfResults.NumOfResults = NumOfResults.Many)


object NumOfResults extends Enumeration {
  type NumOfResults = Value
  val ZeroOrOne, One, Many = Value
}

object DbSchema {
  def read(url: String, driver: String, username: String, password: String): ?[Schema] = try {
    Class.forName(driver)
    val options = new SchemaCrawlerOptions
    val level = new SchemaInfoLevel
    level.setRetrieveTables(true)
    level.setRetrieveColumnDataTypes(true)
    level.setRetrieveTableColumns(true)
    level.setRetrieveIndices(true)
    level.setRetrieveForeignKeys(true)
    options.setSchemaInfoLevel(level)
    val schemaName = url.split('?')(0).split('/').reverse.head
    options.setSchemaInclusionRule(new InclusionRule(schemaName, ""))
    val conn = getConnection(url, username, password)
    val database = SchemaCrawlerUtility.getDatabase(conn, options)
    database.getSchema(schemaName).ok
  } catch {
    case e: Exception => fail(e.getMessage)
  }

  private def getConnection(url: String, username: String, password: String) =
    java.sql.DriverManager.getConnection(url, username, password)
}

object Variables extends Ast.Resolved {
  def input(schema: Schema, stmt: Statement): List[Named] = stmt match {
    case Delete(from, where) => where.map(w => params(w.expr)).getOrElse(Nil)

    case Insert(table, colNames, insertInput) =>
      def colNamesFromSchema = schema.getTable(table.name).getColumns.toList.map(_.getName)

      insertInput match {
        case ListedInput(values) => 
          colNames getOrElse colNamesFromSchema zip values collect {
            case (name, Input()) => Named(name, None, Column(name, table))
          }
        case SelectedInput(select) => input(schema, select)
      }

    case Union(left, right, orderBy, limit) => 
      input(schema, left) ::: input(schema, right) ::: limitParams(limit)

    case Update(tables, set, where, orderBy, limit) => 
      set.collect { case (col, Input()) => Named(col.name, None, col) } :::
      where.map(w => params(w.expr)).getOrElse(Nil) ::: 
      limitParams(limit)

    case Create() => Nil

    case Select(projection, from, where, groupBy, orderBy, limit) =>
      projection.collect { case Named(n, a, f@Function(_, _)) => input(f) }.flatten :::
      where.map(w => params(w.expr)).getOrElse(Nil) ::: 
      groupBy.flatMap(g => g.having.map(h => params(h.expr))).getOrElse(Nil) :::
      limitParams(limit)
  }

  def input(f: Function): List[Named] = f.params flatMap {
    case Input() => Named("<farg>", None, Constant[Table](typeOf[AnyRef], ())) :: Nil // FIXME can be typed
    case f2@Function(_, _) => input(f2)
    case _ => Nil
  }

  def output(stmt: Statement): List[Named] = stmt match {
    case Delete(_, _) => Nil
    case Insert(_, _, _) => Nil
    case Union(left, _, _, _) => output(left)
    case Update(_, _, _, _, _) => Nil
    case Create() => Nil
    case Select(projection, _, _, _, _, _) => projection
  }

  def params(e: Expr): List[Named] = e match {
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

  def limitParams(limit: Option[Limit]) =
    limit.map(l => l.count.right.toSeq.toList ::: l.offset.map(_.right.toSeq.toList).getOrElse(Nil))
      .getOrElse(Nil).map(_ => Named("<constant>", None, Constant[Table](typeOf[Long], None)))
}

class Typer(schema: Schema, stmt: Ast.Statement[Table]) extends Ast.Resolved {
  def infer(useInputTags: Boolean): ?[TypedStatement] = {
    def tag(col: Column) = {
      getTable(col.table) map { t =>
        def findFK = t.getForeignKeys
          .flatMap(_.getColumnPairs.map(_.getForeignKeyColumn))
          .find(_.getName == col.name)
          .map(_.getReferencedColumn.getParent.getName)

        if (t.getPrimaryKey != null && t.getPrimaryKey.getColumns.exists(_.getName == col.name))
          Some(col.table.name)
        else findFK orElse None
      }
    }

    def typeValue(useTags: Boolean)(x: Named): ?[List[TypedValue]] = x.value match {
      case col@Column(_, _) => 
        for {
          (tpe, opt) <- inferColumnType(col)
          t <- tag(col)
        } yield List(TypedValue(x.aname, tpe, opt, if (useTags) t else None))
      case AllColumns(t) =>
        for {
          tbl <- getTable(t)
          cs  <- sequence(tbl.getColumns.toList map { c => typeValue(useTags)(Named(c.getName, None, Column(c.getName, t))) })
        } yield cs.flatten
      case Function(name, params) =>
        inferReturnType(name, params) map { case (tpe, opt) =>
          List(TypedValue(x.aname, tpe, opt, None))
        }
      case Constant(tpe, _) => List(TypedValue(x.aname, tpe, false, None)).ok
      case ArithExpr(lhs, _, rhs) => 
        (lhs, rhs) match {
          case (c@Column(_, _), _) => typeValue(useTags)(Named(c.name, x.alias, c))
          case (_, c@Column(_, _)) => typeValue(useTags)(Named(c.name, x.alias, c))
          case (Constant(tpe, _), _) if tpe == typeOf[Double] => typeValue(useTags)(Named(x.name, x.alias, lhs))
          case (_, Constant(tpe, _)) if tpe == typeOf[Double] => typeValue(useTags)(Named(x.name, x.alias, lhs))
          case (c@Constant(_, _), _) => List(TypedValue(x.aname, typeOf[Int], false, None)).ok
          case (_, c@Constant(_, _)) => List(TypedValue(x.aname, typeOf[Int], false, None)).ok
          case _ => typeValue(useTags)(Named(x.name, x.alias, lhs))
        }
    }

    def uniqueConstraints = {
      val constraints = sequence(stmt.tables map { t =>
        getTable(t) map { table =>
          val indices = Option(table.getPrimaryKey).map(List(_)).getOrElse(Nil) ::: table.getIndices.toList
          val uniques = indices filter (_.isUnique) map { i =>
            i.getColumns.toList.map(col => Column(col.getName, t))
          }
          (t, uniques)
        }
      })

      constraints map (cs => Map[Table, List[List[Column]]]().withDefault(_ => Nil) ++ cs)
    }

    def generatedKeyTypes(table: Table) = for {
      t <- getTable(table)
    } yield {
      def tag(c: schemacrawler.schema.Column) = 
        Option(t.getPrimaryKey).flatMap(_.getColumns.find(_.getName == c.getName)).map(_ => t.getName)

      t.getColumns.toList
        .filter(c => c.getType.isAutoIncrementable)
        .map(c => TypedValue(c.getName, mkType(c.getType), false, tag(c)))
    }

    for {
      in  <- sequence(Variables.input(schema, stmt) map typeValue(useTags = useInputTags))
      out <- sequence(Variables.output(stmt) map typeValue(useTags = true))
      ucs <- uniqueConstraints
      key <- generatedKeyTypes(stmt.tables.head)
    } yield TypedStatement(in.flatten, out.flatten, stmt, ucs, key)
  }

  def isAggregate(fname: String): Boolean = aggregateFunctions.contains(fname)

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
  ) ++ extraScalarFunctions

  val knownFunctions = aggregateFunctions ++ scalarFunctions

  def extraAggregateFunctions: Map[String, (String, List[Term]) => ?[(Type, Boolean)]] = Map()
  def extraScalarFunctions: Map[String, (String, List[Term]) => ?[(Type, Boolean)]] = Map()

  def tpeOf(e: Term): ?[(Type, Boolean)] = e match {
    case Constant(tpe, x) if x == null => (tpe, true).ok
    case Constant(tpe, _)              => (tpe, false).ok
    case col@Column(_, _)              => inferColumnType(col)
    case Function(n, params)           => inferReturnType(n, params)
    case Input()                       => (typeOf[AnyRef], false).ok
    case x                             => sys.error("Term " + x + " not supported")
  }

  def inferReturnType(fname: String, params: List[Term]) = 
    knownFunctions.get(fname.toLowerCase) match {
      case Some(f) => f(fname, params)
      case None => (typeOf[AnyRef], true).ok
    }

  def inferColumnType(col: Column) = for {
    t <- getTable(col.table)
    c <- Option(t.getColumn(col.name)) orFail ("No such column " + col)
  } yield (mkType(c.getType), c.isNullable)

  private def getTable(table: Table) =
    Option(schema.getTable(table.name)) orFail ("Unknown table " + table.name)

  private def mkType(t: ColumnDataType): Type = t.getTypeClassName match {
    case "java.lang.String" => typeOf[String]
    case "java.lang.Short" => typeOf[Short]
    case "java.lang.Integer" => typeOf[Int]
    case "java.lang.Long" => typeOf[Long]
    case "java.lang.Float" => typeOf[Float]
    case "java.lang.Double" => typeOf[Double]
    case "java.lang.Boolean" => typeOf[Boolean]
    case "java.lang.Byte" => typeOf[Byte]
    case "java.sql.Timestamp" => typeOf[java.sql.Timestamp]
    case "java.sql.Date" => typeOf[java.sql.Date]
    case "java.sql.Time" => typeOf[java.sql.Time]
    case "byte[]" => typeOf[java.sql.Blob]
    case x => sys.error("Unknown type " + x)  // FIXME improve error handling
  }
}

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
  , multipleResults: Boolean = true)

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

object Typer extends Ast.Resolved {
  def infer(schema: Schema, stmt: Statement, useInputTags: Boolean): ?[TypedStatement] = {
    def tag(col: Column) = {
      getTable(schema, col.table) map { t =>
        def findFK = t.getForeignKeys
          .flatMap(_.getColumnPairs.map(_.getForeignKeyColumn))
          .find(_.getName == col.name)
          .map(_.getReferencedColumn.getParent.getName)

        if (t.getPrimaryKey != null && t.getPrimaryKey.getColumns.exists(_.getName == col.name))
          Some(col.table.name)
        else findFK orElse None
      }
    }

    def typeValue(inputArg: Boolean, useTags: Boolean)(x: Named): ?[List[TypedValue]] = x.value match {
      case col@Column(_, _) => 
        for {
          (tpe, inopt, outopt) <- inferColumnType(schema, stmt, col)
          t <- tag(col)
        } yield List(TypedValue(x.aname, tpe, if (inputArg) inopt else outopt, if (useTags) t else None))
      case cols@AllColumns(t) =>
        for {
          tbl <- getTable(schema, t)
          cs  <- sequence(tbl.getColumns.toList map { c => typeValue(inputArg, useTags)(Named(c.getName, None, Column(c.getName, t))) })
        } yield cs.flatten
      case f@Function(name, params) =>
        inferReturnType(schema, stmt, name, params) map { case (tpe, inopt, outopt) =>
          List(TypedValue(x.aname, tpe, if (inputArg) inopt else outopt, None))
        }
      case c@Constant(tpe, _) => List(TypedValue(x.aname, tpe, false, None)).ok
      case ArithExpr(lhs, _, rhs) => 
        (lhs, rhs) match {
          case (c@Column(_, _), _) => typeValue(inputArg, useTags)(Named(c.name, x.alias, c))
          case (_, c@Column(_, _)) => typeValue(inputArg, useTags)(Named(c.name, x.alias, c))
          case (Constant(tpe, _), _) if tpe == typeOf[Double] => typeValue(inputArg, useTags)(Named(x.name, x.alias, lhs))
          case (_, Constant(tpe, _)) if tpe == typeOf[Double] => typeValue(inputArg, useTags)(Named(x.name, x.alias, lhs))
          case (c@Constant(_, _), _) => List(TypedValue(x.aname, typeOf[Int], false, None)).ok
          case (_, c@Constant(_, _)) => List(TypedValue(x.aname, typeOf[Int], false, None)).ok
          case _ => typeValue(inputArg, useTags)(Named(x.name, x.alias, lhs))
        }
    }

    def uniqueConstraints = {
      val constraints = sequence(stmt.tables map { t =>
        getTable(schema, t) map { table =>
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
      t <- getTable(schema, table)
    } yield {
      def tag(c: schemacrawler.schema.Column) = 
        Option(t.getPrimaryKey).flatMap(_.getColumns.find(_.getName == c.getName)).map(_ => t.getName)

      t.getColumns.toList
        .filter(c => c.getType.isAutoIncrementable)
        .map(c => TypedValue(c.getName, mkType(c.getType), false, tag(c)))
    }

    for {
      in  <- sequence(input(schema, stmt) map typeValue(inputArg = true, useTags = useInputTags))
      out <- sequence(output(stmt) map typeValue(inputArg = false, useTags = true))
      ucs <- uniqueConstraints
      key <- generatedKeyTypes(stmt.tables.head)
    } yield TypedStatement(in.flatten, out.flatten, stmt, ucs, key)
  }

  def `a => a` = (schema: Schema, stmt: Statement, params: List[Term]) =>
    if (params.length != 1) fail("Expected 1 parameter " + params)
    else 
      tpeOf(schema, stmt, params.head) map { case (tpe, inopt, _) => (tpe, inopt, true) }

  def `_ => ?`(tpe: Type, inopt: Boolean, outopt: Boolean) = 
    (schema: Schema, stmt: Statement, params: List[Term]) => (tpe, inopt, outopt).ok

  // FIXME make this extensible
  val knownFunctions = Map(
      "abs"   -> `a => a`
    , "avg"   -> `_ => ?`(typeOf[Double], false, true)
    , "count" -> `_ => ?`(typeOf[Long], false, false)
    , "min"   -> `a => a`
    , "max"   -> `a => a`
    , "sum"   -> `a => a`
    , "upper" -> `_ => ?`(typeOf[String], false, true)
  )

  def tpeOf(schema: Schema, stmt: Statement, e: Term): ?[(Type, Boolean, Boolean)] = e match {
    case Constant(tpe, _)    => (tpe, false, false).ok
    case col@Column(_, _)    => inferColumnType(schema, stmt, col)
    case Function(n, params) => inferReturnType(schema, stmt, n, params)
    case x                   => sys.error("Term " + x + " not supported")
  }

  def inferReturnType(schema: Schema, stmt: Statement, fname: String, params: List[Term]) = 
    knownFunctions.get(fname.toLowerCase) match {
      case Some(f) => f(schema, stmt, params)
      case None => (typeOf[AnyRef], true, true).ok
    }

  def inferColumnType(schema: Schema, stmt: Statement, col: Column) = for {
    t <- getTable(schema, col.table)
    c <- Option(t.getColumn(col.name)) resultOrFail ("No such column " + col)
  } yield (mkType(c.getType), c.isNullable, c.isNullable)

  private def getTable(schema: Schema, table: Table) =
    Option(schema.getTable(table.name)) resultOrFail ("Unknown table " + table.name)

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

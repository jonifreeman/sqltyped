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
  , stmt: Statement
  , uniqueConstraints: Map[Table, List[List[Column]]]
  , generatedKeyTypes: List[TypedValue]
  , multipleResults: Boolean = true)

object DbSchema {
  def read(url: String, driver: String, username: String, password: String): Schema = {
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
    database.getSchema(schemaName)
  }

  private def getConnection(url: String, username: String, password: String) =
    java.sql.DriverManager.getConnection(url, username, password)
}

// FIXME add error handling
object Typer {
  def infer(schema: Schema, stmt: Statement, useInputTags: Boolean): Result[TypedStatement] = {
    def tag(col: Column) = {
      val table = col.resolvedTable getOrElse sys.error("Column's table not resolved " + col)
      val t = getTable(schema, table)

      def findFK = t.getForeignKeys
        .flatMap(_.getColumnPairs.map(_.getForeignKeyColumn))
        .find(_.getName == col.name)
        .map(_.getReferencedColumn.getParent.getName)

      if (t.getPrimaryKey != null && t.getPrimaryKey.getColumns.exists(_.getName == col.name))
        Some(table.name)
      else findFK orElse None
    }

    def typeValue(inputArg: Boolean, useTags: Boolean)(x: Value): List[TypedValue] = x match {
      case col: Column =>
        val (tpe, inopt, outopt) = inferColumnType(schema, stmt, col)
        List(TypedValue(col.aname, tpe, if (inputArg) inopt else outopt, if (useTags) tag(col) else None))
      case cols@AllColumns(_, t) =>
        val table = t getOrElse sys.error("Table not resolved for " + cols)
        getTable(schema, table).getColumns.toList flatMap { c =>
          typeValue(inputArg, useTags)(Column(c.getName, None, None, Some(table)))
        }
      case f@Function(name, params, alias) =>
        val (tpe, inopt, outopt) = inferReturnType(schema, stmt, name, params)
        List(TypedValue(f.aname, tpe, if (inputArg) inopt else outopt, None))
      case c@Constant(tpe, _) => List(TypedValue("<constant>", tpe, false, None))
      case ArithExpr(lhs, _, rhs) =>
        val lval = typeValue(inputArg, useTags)(lhs).head
        val rval = typeValue(inputArg, useTags)(rhs).head
        (lval, rval) match {
          case (col: Column, _) => List(col)
          case (_, col: Column) => List(col)
          case (c: Constant, _) if c.tpe == typeOf[Double] => List(c)
          case (_, c: Constant) if c.tpe == typeOf[Double] => List(c)
          case (c: Constant, _) => List(TypedValue("<constant>", typeOf[Int], false, None))
          case (_, c: Constant) => List(TypedValue("<constant>", typeOf[Int], false, None))
          case _ => typeValue(inputArg, useTags)(lhs)
        }
    }

    def uniqueConstraints = 
      Map[Table, List[List[Column]]]().withDefault(_ => Nil) ++ (stmt.tables map { t =>
        val table = getTable(schema, t)
        val indices = Option(table.getPrimaryKey).map(List(_)).getOrElse(Nil) ::: table.getIndices.toList
        val uniques = indices filter (_.isUnique) map { i =>
          i.getColumns.toList.map(col => Column(col.getName, Some(t.name), None, Some(t)))
        }
        (t, uniques)
      })

    def generatedKeyTypes(table: Table) = {
      val t = getTable(schema, table)

      def tag(c: schemacrawler.schema.Column) = 
        Option(t.getPrimaryKey).flatMap(_.getColumns.find(_.getName == c.getName)).map(_ => t.getName)

      t.getColumns.toList
        .filter(c => c.getType.isAutoIncrementable)
        .map(c => TypedValue(c.getName, mkType(c.getType), false, tag(c)))
    }

    TypedStatement(stmt.input(schema) flatMap typeValue(inputArg = true, useTags = useInputTags), 
                   stmt.output flatMap typeValue(inputArg = false, useTags = true), 
                   stmt, 
                   uniqueConstraints,
                   generatedKeyTypes(stmt.tables.head)).ok
  }

  def `a => a` = (schema: Schema, stmt: Statement, params: List[Term]) =>
    if (params.length != 1) sys.error("Expected 1 parameter " + params)
    else {
      val (tpe, inopt, _) = tpeOf(schema, stmt, params.head)
      (tpe, inopt, true)
    }

  // FIXME make this extensible
  val knownFunctions = Map(
      "abs"   -> `a => a`
    , "avg"   -> ((_: Schema, _: Statement, _: List[Term]) => (typeOf[Double], false, true))
    , "count" -> ((_: Schema, _: Statement, _: List[Term]) => (typeOf[Long], false, false))
    , "min"   -> `a => a`
    , "max"   -> `a => a`
    , "sum"   -> `a => a`
    , "upper" -> ((_: Schema, _: Statement, _: List[Term]) => (typeOf[String], false, true))
  )

  def tpeOf(schema: Schema, stmt: Statement, e: Term): (Type, Boolean, Boolean) = e match {
    case Constant(tpe, _)       => (tpe, false, false)
    case col: Column            => inferColumnType(schema, stmt, col)
    case Function(n, params, _) => inferReturnType(schema, stmt, n, params)
    case x                      => sys.error("Term " + x + " not supported yet") // FIXME
  }

  // FIXME emit warning for unknown function
  def inferReturnType(schema: Schema, stmt: Statement, fname: String, params: List[Term]) = 
    knownFunctions.get(fname.toLowerCase)
      .map(f => f(schema, stmt, params))
      .getOrElse((typeOf[AnyRef], true, true))

  def inferColumnType(schema: Schema, stmt: Statement, col: Column) = {
    val table = col.resolvedTable getOrElse sys.error("Table not resolved for " + col)
    val colSchema = getTable(schema, table).getColumn(col.name)
    if (colSchema == null) sys.error("No such column " + col)
    (mkType(colSchema.getType), colSchema.isNullable, colSchema.isNullable)
  }

  private def getTable(schema: Schema, table: Table) =
    Option(schema.getTable(table.name)) getOrElse sys.error("Unknown table " + table.name)

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

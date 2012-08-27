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
  , multipleResults: Boolean = true)

// FIXME add error handling
object Typer {
  def infer(stmt: Statement, url: String, driver: String, username: String, password: String): TypedStatement = {
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
    val schema = database.getSchema(schemaName)

    def tag(col: Column) = {
      None
/*
      val table = stmt.tableOf(col).getOrElse(sys.error("Column references invalid table " + col))
      val t = schema.getTable(table.name)
      if (t.getPrimaryKey != null && t.getPrimaryKey.getColumns.exists(_.getName == col.name))
        Some(table.name)
      else if (t.getForeignKeys.exists(_.getColumnPairs.exists(_.getForeignKeyColumn.getName == col.name)))
        Some(???)
      else 
        None */
    }

    def typeValue(x: Value) = x match {
      case col: Column =>
        val (tpe, nullable) = inferColumnType(schema, stmt, col)        
        TypedValue(col.aname, tpe, nullable, tag(col))
      case f@Function(name, params, alias) =>
        val (tpe, nullable) = inferReturnType(schema, stmt, name, params)
        TypedValue(f.aname, tpe, nullable, None)
      case c@Constant(tpe, _) => TypedValue("<constant>", tpe, false, None)
    }

    def uniqueConstraints = 
      Map[Table, List[List[Column]]]().withDefault(_ => Nil) ++ (stmt.tables map { t =>
        val table = schema.getTable(t.name)
        val indices = Option(table.getPrimaryKey).map(List(_)).getOrElse(Nil) ::: table.getIndices.toList
        val uniques = indices filter (_.isUnique) map { i =>
          i.getColumns.toList.map(col => Column(col.getName, Some(t.name)))
        }
        (t, uniques)
      })

    TypedStatement(stmt.input map typeValue, stmt.output map typeValue, stmt, uniqueConstraints)
  }

  val `a => a` = (schema: Schema, stmt: Statement, params: List[Term]) =>
    if (params.length != 1) sys.error("Expected 1 parameter " + params)
    else tpeOf(schema, stmt, params.head)

  // FIXME make this extensible
  val knownFunctions = Map(
      "abs"   -> (`a => a`, true)
    , "avg"   -> ((_: Schema, _: Statement, _: List[Term]) => typeOf[Double], true)
    , "count" -> ((_: Schema, _: Statement, _: List[Term]) => typeOf[Long], false)
    , "min"   -> (`a => a`, true)
    , "max"   -> (`a => a`, true)
    , "sum"   -> (`a => a`, true)
  )

  def tpeOf(schema: Schema, stmt: Statement, e: Term): Type = e match {
    case Constant(tpe, _)       => tpe
    case col: Column            => inferColumnType(schema, stmt, col)._1
    case Function(n, params, _) => inferReturnType(schema, stmt, n, params)._1
    case x                      => sys.error("Term " + x + " not supported yet") // FIXME
  }

  // FIXME emit warning for unknown function
  def inferReturnType(schema: Schema, stmt: Statement, fname: String, params: List[Term])  = 
    knownFunctions.get(fname.toLowerCase)
      .map { case (f, opt) => (f(schema, stmt, params), opt) }
      .getOrElse((typeOf[AnyRef], true))

  def inferColumnType(schema: Schema, stmt: Statement, col: Column) = {
    val table = stmt.tableOf(col).map(_.name).getOrElse(sys.error("No table for " + col))
    val colSchema = schema.getTable(table).getColumn(col.name)
    if (colSchema == null) sys.error("No such column " + col)
    (mkType(colSchema.getType), colSchema.isNullable)
  }

  private def getConnection(url: String, username: String, password: String) =
    java.sql.DriverManager.getConnection(url, username, password)

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
    case x => sys.error("Unknown type " + x)  // FIXME improve error handling
  }
}

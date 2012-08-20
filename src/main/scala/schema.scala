package sqltyped

import schemacrawler.schemacrawler._
import schemacrawler.schema._
import schemacrawler.utility.SchemaCrawlerUtility
import scala.reflect.runtime.universe._

case class TypedExpr(expr: Expr, tpe: Type, nullable: Boolean) {
  def name = expr.name
}

case class SqlMeta(input: List[TypedExpr], output: List[TypedExpr])

// FIXME add error handling
object Schema {
  def infer(select: Select, url: String, driver: String, username: String, password: String): SqlMeta = {
    Class.forName(driver)
    val options = new SchemaCrawlerOptions
    val level = new SchemaInfoLevel
    level.setRetrieveTables(true)
    level.setRetrieveColumnDataTypes(true)
    level.setRetrieveTableColumns(true)
    options.setSchemaInfoLevel(level)
    val conn = getConnection(url, username, password)
    val database = SchemaCrawlerUtility.getDatabase(conn, options)
    val schemaName = url.split('?')(0).split('/').reverse.head
    val schema = database.getSchema(schemaName)

    def typeExpr(expr: Expr) = expr match {
      case col: Column =>
        val (tpe, nullable) = inferColumnType(schema, col)
        TypedExpr(col, tpe, nullable)
      case f@Function(fname, params, alias) =>
        val (tpe, nullable) = inferReturnType(schema, fname, params)
        TypedExpr(f, tpe, nullable)
      case c@Constant(tpe) => TypedExpr(c, tpe, false)
    }

    SqlMeta(select.in map typeExpr, select.out map typeExpr)
  }

  val `a => a` = (schema: Schema, params: List[Expr]) =>
    if (params.length != 1) sys.error("Expected 1 parameter " + params)
    else tpeOf(schema, params.head)

  val knownFunctions = Map(
      "abs"   -> (`a => a`, true)
    , "avg"   -> ((_: Schema, _: List[Expr]) => typeOf[Double], true)
    , "count" -> ((_: Schema, _: List[Expr]) => typeOf[Long], false)
    , "min"   -> (`a => a`, true)
    , "max"   -> (`a => a`, true)
    , "sum"   -> (`a => a`, true)
  )

  def tpeOf(schema: Schema, e: Expr): Type = e match {
    case Constant(tpe)          => tpe
    case col: Column            => inferColumnType(schema, col)._1
    case Function(n, params, _) => inferReturnType(schema, n, params)._1
  }

  // FIXME emit warning for unknown function
  def inferReturnType(schema: Schema, fname: String, params: List[Expr])  = 
    knownFunctions.get(fname.toLowerCase)
      .map { case (f, opt) => (f(schema, params), opt) }
      .getOrElse((typeOf[AnyRef], true))

  def inferColumnType(schema: Schema, col: Column) = {
    val colSchema = schema.getTable(col.table).getColumn(col.cname)
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

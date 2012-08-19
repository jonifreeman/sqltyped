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
      case col@Column(table, cname, alias) => 
        val colSchema = schema.getTable(table).getColumn(cname)
        if (colSchema == null) sys.error("No such column " + col)
        TypedExpr(col, mkType(colSchema.getType), colSchema.isNullable)
      case f@Function(fname, alias) =>
        val (tpe, nullable) = 
          inferReturnType(fname) getOrElse sys.error("Do not know return type of " + fname)
        TypedExpr(f, tpe, nullable)
    }

    SqlMeta(select.in map typeExpr, select.out map typeExpr)
  }

  // FIXME functions can be polymorphic (e.g. abs :: a -> a)
  private val knownFunctions = Map(
      "abs"   -> (typeOf[Long], true)
    , "avg"   -> (typeOf[Double], true)
    , "count" -> (typeOf[Long], false)
    , "sum"   -> (typeOf[Long], true)
  )

  private def inferReturnType(fname: String) = knownFunctions.get(fname.toLowerCase)

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

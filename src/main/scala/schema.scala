package sqltyped

import schemacrawler.schemacrawler._
import schemacrawler.schema._
import schemacrawler.utility.SchemaCrawlerUtility
import scala.reflect.runtime.universe._

case class SqlStmt(columns: List[Column])

case class SqlMeta(columns: List[(String, Type, Boolean)])

// FIXME add error handling
object Schema {
  def infer(stmt: SqlStmt, url: String, driver: String, username: String, password: String): SqlMeta = {
    Class.forName(driver)
    val options = new SchemaCrawlerOptions
    options.setSchemaInfoLevel(SchemaInfoLevel.standard)
    val conn = createConnection(url, username, password)
    val database = SchemaCrawlerUtility.getDatabase(conn, options)
    val schema = database.getSchema("sqltyped") // FIXME hardcoded schema
    SqlMeta(stmt.columns map { col =>
      val colSchema = schema.getTable(col.table).getColumn(col.name)
      (col.name, mkType(colSchema.getType), colSchema.isNullable)
    })
  }

  private def createConnection(url: String, username: String, password: String) = {
    val props = new java.util.Properties
    props.put("user", username)
    props.put("password", password)
    java.sql.DriverManager.getConnection(url, props)
  }

  // FIXME add rest of the types
  private def mkType(t: ColumnDataType): Type = t.getTypeClassName match {
    case "java.lang.String" => typeOf[String]
    case "java.lang.Integer" => typeOf[Int]
    case x => sys.error("Unknown type " + x)  // FIXME improve error handling
  }
}

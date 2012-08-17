package sqltyped

import schemacrawler.schemacrawler._
import schemacrawler.schema._
import schemacrawler.utility.SchemaCrawlerUtility
import scala.reflect.runtime.universe._

case class TypedColumn(column: Column, tpe: Type, nullable: Boolean)

case class SqlMeta(columns: List[TypedColumn], input: List[TypedColumn])

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
    val conn = createConnection(url, username, password)
    val database = SchemaCrawlerUtility.getDatabase(conn, options)
    val schemaName = url.split('?')(0).split('/').reverse.head
    val schema = database.getSchema(schemaName)

    def typeColumn(col: Column) = {
      val colSchema = schema.getTable(col.table).getColumn(col.cname)
      if (colSchema == null) sys.error("No such column " + col)
      TypedColumn(col, mkType(colSchema.getType), colSchema.isNullable)
    }

    SqlMeta(select.out map typeColumn, select.in map typeColumn)
  }


  private def createConnection(url: String, username: String, password: String) = 
    java.sql.DriverManager.getConnection(url, username, password)

  // FIXME add rest of the types
  private def mkType(t: ColumnDataType): Type = t.getTypeClassName match {
    case "java.lang.String" => typeOf[String]
    case "java.lang.Integer" => typeOf[Int]
    case x => sys.error("Unknown type " + x)  // FIXME improve error handling
  }
}

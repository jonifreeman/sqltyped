package sqltyped

import schemacrawler.schemacrawler._
import schemacrawler.schema.Schema
import schemacrawler.utility.SchemaCrawlerUtility

case class DbConfig(url: String, driver: String, username: String, password: String, schema: Option[String]) {
  def getConnection = java.sql.DriverManager.getConnection(url, username, password)
}

object DbSchema {
  def read(config: DbConfig): ?[Schema] = try {    
    Class.forName(config.driver)
    val options = new SchemaCrawlerOptions
    val level = new SchemaInfoLevel
    level.setRetrieveTables(true)
    level.setRetrieveColumnDataTypes(true)
    level.setRetrieveTableColumns(true)
    level.setRetrieveIndices(true)
    level.setRetrieveForeignKeys(true)
    options.setSchemaInfoLevel(level)
    val schemaName = config.schema getOrElse config.url.split('?')(0).split('/').reverse.head
    options.setSchemaInclusionRule(new InclusionRule(schemaName, ""))
    val conn = config.getConnection
    val database = SchemaCrawlerUtility.getDatabase(conn, options)
    Option(database.getSchema(schemaName)) orFail 
      s"Can't read schema '$schemaName'. Schema name can be configured with system property 'sqltyped.schema'."
  } catch {
    case e: Exception => fail(e.getMessage)
  }
}


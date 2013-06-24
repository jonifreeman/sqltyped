package sqltyped

import Ast._

trait Validator {
  def validate(db: DbConfig, sql: String): ?[Unit]
}

object NOPValidator extends Validator {
  def validate(db: DbConfig, sql: String) = ().ok
}

object JdbcValidator extends Validator {
  def validate(db: DbConfig, sql: String) = 
    Jdbc.withConnection(db.getConnection) { conn =>
      val stmt = conn.prepareStatement(sql)
      stmt.getMetaData // some JDBC drivers do round trip to DB here and validates the statement
    }
}

/**
 * For MySQL we use its internal API to get better validation.
 */
object MySQLValidator extends Validator {
  def validate(db: DbConfig, sql: String) = try {
    Jdbc.withConnection(db.getConnection) { conn =>
      val m = Class.forName("com.mysql.jdbc.ServerPreparedStatement").getDeclaredMethod(
        "getInstance", 
        Class.forName("com.mysql.jdbc.MySQLConnection"), 
        classOf[String], 
        classOf[String], 
        classOf[Int], 
        classOf[Int])
      m.setAccessible(true)
      m.invoke(null, conn, sql, "", 0: java.lang.Integer, 0: java.lang.Integer).ok
    }
  } catch {
    case e: Exception if e.getClass.getName.endsWith("MySQLSyntaxErrorException") => fail(e.getMessage)
    case e: Exception => JdbcValidator.validate(db, sql)
  }
}

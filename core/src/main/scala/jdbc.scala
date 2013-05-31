package sqltyped

import java.sql._
import scala.reflect.runtime.universe.{Type, typeOf}

private[sqltyped] object Jdbc {
  def infer(db: DbConfig, sql: String): ?[TypedStatement] = 
    withConnection(db.getConnection) { conn =>
      val stmt = conn.prepareStatement(sql)
      for {
        out <- (Option(stmt.getMetaData) map inferOutput getOrElse Nil).ok
        in  <- Option(stmt.getParameterMetaData) map inferInput orFail "Input metadata not available"
        isQuery = !out.isEmpty
      } yield TypedStatement(in, out, isQuery, Map(), Nil, if (isQuery) NumOfResults.Many else NumOfResults.One)
    } flatMap identity

  def inferInput(meta: ParameterMetaData) = 
    (1 to meta.getParameterCount).toList map { i => 
      try {
        TypedValue("a" + i, mkType(meta.getParameterClassName(i)), 
                   meta.isNullable(i) == ParameterMetaData.parameterNullable, None)
      } catch {
        case e: SQLException => TypedValue("a" + i, typeOf[Any], false, None)
      }
    }

  def inferOutput(meta: ResultSetMetaData) = 
    (1 to meta.getColumnCount).toList map { i => 
      TypedValue(meta.getColumnName(i), mkType(meta.getColumnClassName(i)), 
                 meta.isNullable(i) == ResultSetMetaData.columnNullable, None)
    }

  def withConnection[A](conn: Connection)(a: Connection => A): ?[A] = try { 
    a(conn).ok
  } catch {
    case e: SQLException => fail(e.getMessage)
  } finally { 
    conn.close 
  }

  def mkType(className: String): Type = className match {
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
    case "[B" => typeOf[java.sql.Blob]
    case "byte" => typeOf[Byte]
    case "java.math.BigDecimal" => typeOf[scala.math.BigDecimal]
    case x => sys.error("Unknown type " + x)
  }
}

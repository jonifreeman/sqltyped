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
        TypedValue("a" + i, (mkType(meta.getParameterClassName(i)), meta.getParameterType(i)), 
                   meta.isNullable(i) == ParameterMetaData.parameterNullable, None, unknownTerm)
      } catch {
        case e: SQLException => TypedValue("a" + i, (typeOf[Any], Types.JAVA_OBJECT), false, None, unknownTerm)
      }
    }

  def inferOutput(meta: ResultSetMetaData) = 
    (1 to meta.getColumnCount).toList map { i => 
      TypedValue(meta.getColumnLabel(i), (mkType(meta.getColumnClassName(i)), meta.getColumnType(i)), 
                 meta.isNullable(i) != ResultSetMetaData.columnNoNulls, None, unknownTerm)
    }

  def unknownTerm = Ast.Column("unknown", Ast.Table("unknown", None))

  def withConnection[A](conn: Connection)(a: Connection => A): ?[A] = try { 
    a(conn).ok
  } catch {
    case e: SQLException => fail(e.getMessage)
  } finally { 
    conn.close 
  }

  // FIXME move to TypeMappings
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

private [sqltyped] object TypeMappings {
  import java.sql.Types._

  /* a mapping from java.sql.Types.* values to their getFoo/setFoo names */
  final val setterGetterNames = Map(
      ARRAY         -> "Array"  
    , BIGINT	    -> "Long"   
    , BINARY	    -> "Bytes"  
    , BIT	    -> "Boolean"
    , BLOB	    -> "Blob"   
    , BOOLEAN	    -> "Boolean"
    , CHAR	    -> "String"
    , CLOB	    -> "Clob"
    , DATALINK	    -> "URL"
    , DATE	    -> "Date"
    , DECIMAL	    -> "BigDecimal"
    , DOUBLE	    -> "Double"
    , FLOAT	    -> "Float"
    , INTEGER	    -> "Int"
    , JAVA_OBJECT   -> "Object"
    , LONGNVARCHAR  -> "String"
    , LONGVARBINARY -> "Blob"     // FIXME should be Bytes?
    , LONGVARCHAR   -> "String"
    , NCHAR	    -> "String"
    , NCLOB	    -> "NClob"
    , NUMERIC	    -> "BigDecimal"
    , NVARCHAR	    -> "String"
    , REAL	    -> "Float"
    , REF	    -> "Ref"
    , ROWID	    -> "RowId"
    , SMALLINT	    -> "Short"
    , SQLXML        -> "SQLXML"
    , TIME          -> "Time"
    , TIMESTAMP	    -> "Timestamp"
    , TINYINT	    -> "Byte"
    , VARBINARY	    -> "Bytes"
    , VARCHAR	    -> "String"
  )

  // FIXME this is dialect specific, move there. Or perhaps schemacrawler provides these?
  def arrayTypeName(tpe: Type) = 
    if (tpe =:= typeOf[String]) "varchar"
    else if (tpe =:= typeOf[Int]) "integer"
    else if (tpe =:= typeOf[Long]) "bigint"
    else sys.error("Unsupported array type " + tpe)
}

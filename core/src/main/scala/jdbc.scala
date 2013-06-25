package sqltyped

import java.sql._
import scala.reflect.macros.Context

private[sqltyped] object Jdbc {
  def infer(db: DbConfig, sql: String, context: Context): ?[TypedStatement] = 
    withConnection(db.getConnection) { conn =>
      val stmt = conn.prepareStatement(sql)
      for {
        out <- (Option(stmt.getMetaData) map inferOutput(context) getOrElse Nil).ok
        in  <- Option(stmt.getParameterMetaData) map inferInput(context) orFail "Input metadata not available"
        isQuery = !out.isEmpty
      } yield TypedStatement(in, out, isQuery, Map(), Nil, if (isQuery) NumOfResults.Many else NumOfResults.One)
    } flatMap identity

  private def inferInput(context: Context)(meta: ParameterMetaData) = 
    (1 to meta.getParameterCount).toList map { i => 
      try {
        TypedValue("a" + i, mkType(meta.getParameterClassName(i), context), 
                   meta.isNullable(i) == ParameterMetaData.parameterNullable, None, unknownTerm)
      } catch {
        case e: SQLException => TypedValue("a" + i, context.universe.definitions.AnyTpe, false, None, unknownTerm)
      }
    }

  private def inferOutput(context: Context)(meta: ResultSetMetaData) = 
    (1 to meta.getColumnCount).toList map { i => 
      TypedValue(meta.getColumnName(i), mkType(meta.getColumnClassName(i), context), 
                 meta.isNullable(i) == ResultSetMetaData.columnNullable, None, unknownTerm)
    }

  private def unknownTerm = Ast.Column("unknown", Ast.Table("unknown", None))

  def withConnection[A](conn: Connection)(a: Connection => A): ?[A] = try { 
    a(conn).ok
  } catch {
    case e: SQLException => fail(e.getMessage)
  } finally { 
    conn.close 
  }

  def mkType(className: String, context: Context): context.Type = className match {
    case "byte[]" => context.typeOf[java.sql.Blob]
    case "[B" => context.typeOf[java.sql.Blob]
    case x => context.mirror.staticClass(x).toType
  }
}

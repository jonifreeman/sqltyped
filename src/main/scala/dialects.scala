package sqltyped

import schemacrawler.schema.Schema
import scala.reflect.runtime.universe.Type

trait Dialect {
  def parser: SqlParser
  def typer(schema: Schema, stmt: Ast.Resolved.Statement): Typer // FIXME cleanup, remove params
}

object Dialect {
  def choose(driver: String): Dialect = {
    if (driver.toLowerCase.contains("mysql")) MysqlDialect
    else GenericDialect
  }
}

object GenericDialect extends Dialect {
  val parser = new SqlParser {}
  def typer(schema: Schema, stmt: Ast.Resolved.Statement) = new Typer(schema, stmt)
}

object MysqlDialect extends Dialect {
  def typer(schema: Schema, stmt: Ast.Resolved.Statement) = new Typer(schema, stmt) {
    import dsl._

    override def extraScalarFunctions = Map(
        "datediff"  -> (f2(date, date) -> option(int))
      , "ifnull"    -> ifnull _
      , "coalesce"  -> ifnull _
    )

    def ifnull(fname: String, params: List[Term]): ?[(Type, Boolean)] = 
      if (params.length != 2) fail("Expected 2 parameters " + params)
      else for {
        (tpel, _) <- tpeOf(params(0))
        (_, optr) <- tpeOf(params(1))
      } yield (tpel, optr)
  }

  val parser = MysqlParser

  object MysqlParser extends SqlParser {
    import scala.reflect.runtime.universe.typeOf

    override def extraValues = MysqlParser.interval

    lazy val interval = "interval".i ~> numericLit ~ timeUnit ^^ { case x ~ _ => const(typeOf[java.util.Date], x) }

    lazy val timeUnit = (
        "microsecond".i 
      | "second".i 
      | "minute".i 
      | "hour".i 
      | "day".i 
      | "week".i 
      | "month".i 
      | "quarter".i 
      | "year".i
    )
  }
}

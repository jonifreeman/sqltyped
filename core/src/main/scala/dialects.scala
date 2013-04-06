package sqltyped

import schemacrawler.schema.Schema
import scala.reflect.runtime.universe.{Type, typeOf, glb}
import Ast._

trait Dialect {
  def parser: SqlParser
  def validator: Validator
  def typer(schema: Schema, stmt: Statement[Table]): Typer
}

object Dialect {
  def choose(driver: String): Dialect = {
    if (driver.toLowerCase.contains("mysql")) MysqlDialect
    else GenericDialect
  }
}

object GenericDialect extends Dialect {
  val parser = new SqlParser {}
  def validator = JdbcValidator
  def typer(schema: Schema, stmt: Statement[Table]) = new Typer(schema, stmt)
}

object MysqlDialect extends Dialect {
  def validator = MySQLValidator

  def typer(schema: Schema, stmt: Statement[Table]) = new Typer(schema, stmt) {
    import dsl._

    override def extraScalarFunctions = Map(
        "datediff"  -> datediff _
      , "ifnull"    -> ifnull _
      , "coalesce"  -> ifnull _
      , "if"        -> iff _
    )

    def datediff(fname: String, params: List[Expr]): ?[SqlFType] = 
      if (params.length != 2) fail("Expected 2 parameters " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0))
        (tpe1, opt1) <- tpeOf(params(1))
        tpe = glb(List(tpe0, tpe1, typeOf[java.util.Date]))
      } yield (List((tpe, opt0), (tpe, opt1)), (typeOf[Int], true))

    def ifnull(fname: String, params: List[Expr]): ?[SqlFType] = 
      if (params.length != 2) fail("Expected 2 parameters " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0))
        (tpe1, opt1) <- tpeOf(params(1))
      } yield (List((tpe0, opt0), (tpe1, opt1)), (tpe0, opt1))

    def iff(fname: String, params: List[Expr]): ?[SqlFType] = 
      if (params.length != 3) fail("Expected 3 parameters " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0))
        (tpe1, opt1) <- tpeOf(params(1))
        (tpe2, opt2) <- tpeOf(params(2))
      } yield (List((tpe0, opt0), (tpe1, opt1), (tpe2, opt2)), (tpe1, opt1 || opt2))
  }

  val parser = MysqlParser

  object MysqlParser extends SqlParser {
    import scala.reflect.runtime.universe.typeOf

    override def insert = "insert".i <~ opt("ignore".i)
    override def update = "update".i <~ opt("ignore".i)

    override lazy val insertStmt = insertSyntax ~ opt(onDuplicateKey) ^^ {
      case t ~ cols ~ vals ~ None => Insert(t, cols, vals)
      case t ~ cols ~ vals ~ Some(as) => 
        Composed(Insert(t, cols, vals), Update(t :: Nil, as, None, None, None))
    }

    lazy val onDuplicateKey = 
      "on".i ~> "duplicate".i ~> "key".i ~> "update".i ~> repsep(assignment, ",")

    override def quoteChar = ("\"" | "`")
 
    override def extraTerms = MysqlParser.interval

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

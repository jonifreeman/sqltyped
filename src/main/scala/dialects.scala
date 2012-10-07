package sqltyped

trait Dialect {
//  type T <: Typer
  def parser: SqlParser
}

object Dialect {
  def choose(driver: String): Dialect = {
    if (driver.toLowerCase.contains("mysql")) MysqlDialect
    else GenericDialect
  }
}

object GenericDialect extends Dialect {
  def parser = new SqlParser {}
}

object MysqlDialect extends Dialect {
  val functions = Map(
//      "datediff"  -> Func[(Date, Date), Int]
//      "ifnull"   -> `(a,a) => a`
//    , "coalesce" -> `(a,a) => a`
  )

  def parser = MysqlParser

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

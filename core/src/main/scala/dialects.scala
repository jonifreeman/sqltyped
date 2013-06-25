package sqltyped

import scala.reflect.macros.Context
import Ast._

trait Dialect {
  def parser(context: Context): SqlParser
  def validator: Validator
  def typer(schema: DbSchema, stmt: Statement[Table], context: Context): Typer
}

object Dialect {
  def choose(driver: String): Dialect = {
    if (driver.toLowerCase.contains("mysql")) MysqlDialect
    else GenericDialect
  }
}

object GenericDialect extends Dialect {
  def parser(context: Context) = new SqlParser(context)
  def validator = JdbcValidator
  def typer(schema: DbSchema, stmt: Statement[Table], context: Context) = new Typer(schema, stmt, context)
}

object MysqlDialect extends Dialect {
  def validator = MySQLValidator

  def typer(schema: DbSchema, stmt: Statement[Table], context: Context) = new Typer(schema, stmt, context) {
    import dsl._

    override def extraScalarFunctions = Map(
        "datediff"  -> datediff _
      , "ifnull"    -> ifnull _
      , "coalesce"  -> ifnull _
      , "if"        -> iff _
      , "binary"    -> binary _
      , "convert"   -> convert _
      , "concat"    -> concat _
    )

    private def tpe(t : Context#Type) = t.asInstanceOf[context.Type]

    def datediff(fname: String, params: List[Expr]): ?[SqlFType] = 
      if (params.length != 2) fail("Expected 2 parameters " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0))
        (tpe1, opt1) <- tpeOf(params(1))
        t = context.universe.glb(List(tpe(tpe0), tpe(tpe1), context.typeOf[java.util.Date]))
      } yield (List((t, opt0), (t, opt1)), (context.universe.definitions.IntTpe, true))

    def ifnull(fname: String, params: List[Expr]): ?[SqlFType] = 
      if (params.length != 2) fail("Expected 2 parameters " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0))
        (tpe1, opt1) <- tpeOf(params(1))
      } yield (List((tpe(tpe0), opt0), (tpe(tpe1), opt1)), (tpe(tpe0), opt1))

    def iff(fname: String, params: List[Expr]): ?[SqlFType] = 
      if (params.length != 3) fail("Expected 3 parameters " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0))
        (tpe1, opt1) <- tpeOf(params(1))
        (tpe2, opt2) <- tpeOf(params(2))
      } yield (List((tpe(tpe0), opt0), (tpe(tpe1), opt1), (tpe(tpe2), opt2)), (tpe(tpe1), opt1 || opt2))

    def binary(fname: String, params: List[Expr]): ?[SqlFType] = 
      if (params.length != 1) fail("Expected 1 parameter " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0))
      } yield (List((tpe(tpe0), opt0)), (tpe(tpe0), opt0))

    def convert(fname: String, params: List[Expr]): ?[SqlFType] = 
      if (params.length != 2) fail("Expected 2 parameters " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0))
        (tpe1, opt1) <- tpeOf(params(1))
        (t, opt)   <- castToType(tpe0, params(1))
      } yield (List((tpe(tpe0), opt0), (tpe(tpe1), opt1)), (t, opt0 || opt))

    def concat(fname: String, params: List[Expr]): ?[SqlFType] = 
      if (params.length < 1) fail("Expected at least 1 parameter")
      else for {
        in <- sequence(params map tpeOf)
      } yield (in, (context.typeOf[String], in.map(_._2).forall(identity)))

    private def castToType(orig: Context#Type, target: Expr) = target match {
      case TypeExpr(d) => d.name match {
        case "date" => (context.typeOf[java.sql.Date], true).ok
        case "datetime" => (context.typeOf[java.sql.Timestamp], true).ok
        case "time" => (context.typeOf[java.sql.Time], true).ok
        case "char" => (context.typeOf[String], false).ok
        case "binary" => (context.typeOf[String], false).ok
        case "decimal" => (context.universe.definitions.DoubleTpe, false).ok
        case "signed" if orig == context.universe.definitions.LongTpe => (context.universe.definitions.LongTpe, false).ok
        case "signed" => (context.universe.definitions.IntTpe, false).ok
        case "unsigned" if orig == context.universe.definitions.LongTpe => (context.universe.definitions.LongTpe, false).ok
        case "unsigned" => (context.universe.definitions.IntTpe, false).ok
        case x => fail(s"Unsupported type '$target' in cast operation")
      }
      case e => fail(s"Expected a data type, got '$e'")
    }
  }

  def parser(context: Context) = new MysqlParser(context)

  class MysqlParser(context: Context) extends SqlParser(context) {
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
 
    override def extraTerms = interval

    override def dataTypes = List(
        precision1("binary")
      , precision1("char")
      , precision1("varchar")
      , precision0("tinytext")
      , precision0("text")
      , precision0("blob")
      , precision0("mediumtext")
      , precision0("mediumblob")
      , precision0("longtext")
      , precision0("longblob")
      , precision1("tinyint")
      , precision1("smallint")
      , precision1("mediumint")
      , precision1("int")
      , precision1("bigint")
      , precision0("float")
      , precision2("double")
      , precision2("decimal")
      , precision0("date")
      , precision0("datetime")
      , precision0("timestamp")
      , precision0("time")
      , precision0("signed")
      , precision0("unsigned")
    )

    def precision0(name: String) = name.i ^^ (n => DataType(n))
    def precision1(name: String) = (
        name.i ~ "(" ~ integer ~ ")" ^^ { case n ~ _ ~ l ~ _ => DataType(n, List(l)) } 
      | precision0(name)
    )
    def precision2(name: String) = (
        name.i ~ "(" ~ integer ~ "," ~ integer ~ ")" ^^ { case n ~ _ ~ l1 ~ _ ~ l2 ~ _ => DataType(n, List(l1, l2)) } 
      | precision1(name)
      | precision0(name)
    )

    lazy val intervalAmount = opt("'") ~> numericLit <~ opt("'")
    lazy val interval = "interval".i ~> intervalAmount ~ timeUnit ^^ { case x ~ _ => const(context.typeOf[java.util.Date], x) }

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

package sqltyped

import schemacrawler.schema.Schema
import scala.reflect.runtime.universe.{Type, typeOf, appliedType}
import Ast._
import java.sql.{Types => JdbcTypes}

trait Dialect {
  def parser: SqlParser
  def validator: Validator
  def typer(schema: Schema, stmt: Statement[Table], dbConfig: DbConfig): Typer
}

object Dialect {
  def choose(driver: String): Dialect = {
    if (driver.toLowerCase.contains("mysql")) MysqlDialect
    else if (driver.toLowerCase.contains("postgresql")) PostgresqlDialect
    else GenericDialect
  }
}

object GenericDialect extends Dialect {
  val parser = new SqlParser {}
  def validator = JdbcValidator
  def typer(schema: Schema, stmt: Statement[Table], dbConfig: DbConfig) = new Typer(schema, stmt, dbConfig)
}

object MysqlDialect extends Dialect {
  def validator = MySQLValidator

  def typer(schema: Schema, stmt: Statement[Table], dbConfig: DbConfig) = new Typer(schema, stmt, dbConfig) {
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

    override def typeSpecifyTerm(v: Variable) = v.comparisonTerm flatMap { 
      // Special case for: WHERE col IN (?)
      case Comparison2(t1, In | NotIn, TermList(Input() :: Nil)) => Some(for {
        tpe <- typeTerm(false)(Variable(Named("", None, t1)))
      } yield List(TypedValue(v.term.aname, (appliedType(typeOf[Seq[_]].typeConstructor, tpe.head.tpe._1 :: Nil), JdbcTypes.JAVA_OBJECT), isNullable(t1), None, v.term.term)))
      case _ => None
    }

    def datediff(fname: String, params: List[Expr], ct: Option[Term]): ?[SqlFType] = 
      if (params.length != 2) fail("Expected 2 parameters " + params)
      else for {
        (_tpe0, opt0) <- tpeOf(params(0), ct)
        (_tpe1, opt1) <- tpeOf(params(1), ct)
        tpe0 = if (_tpe0._1 <:< typeOf[java.util.Date]) _tpe0 else _tpe1
        tpe1 = if (_tpe1._1 <:< typeOf[java.util.Date]) _tpe1 else _tpe0
      } yield (List((tpe0, opt0), (tpe1, opt1)), ((typeOf[Int], JdbcTypes.INTEGER), true))

    def ifnull(fname: String, params: List[Expr], ct: Option[Term]): ?[SqlFType] = 
      if (params.length != 2) fail("Expected 2 parameters " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0), ct)
        (tpe1, opt1) <- tpeOf(params(1), ct)
      } yield (List((tpe0, opt0), (tpe1, opt1)), (tpe0, opt1))

    def iff(fname: String, params: List[Expr], ct: Option[Term]): ?[SqlFType] = 
      if (params.length != 3) fail("Expected 3 parameters " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0), ct)
        (tpe1, opt1) <- tpeOf(params(1), ct)
        (tpe2, opt2) <- tpeOf(params(2), ct)
      } yield (List((tpe0, opt0), (tpe1, opt1), (tpe2, opt2)), (tpe1, opt1 || opt2))

    def binary(fname: String, params: List[Expr], ct: Option[Term]): ?[SqlFType] = 
      if (params.length != 1) fail("Expected 1 parameter " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0), ct)
      } yield (List((tpe0, opt0)), (tpe0, opt0))

    def convert(fname: String, params: List[Expr], ct: Option[Term]): ?[SqlFType] = 
      if (params.length != 2) fail("Expected 2 parameters " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0), ct)
        (tpe1, opt1) <- tpeOf(params(1), ct)
        (tpe, opt)   <- castToType(tpe0._1, params(1))
      } yield (List((tpe0, opt0), (tpe1, opt1)), (tpe, opt0 || opt))

    def concat(fname: String, params: List[Expr], ct: Option[Term]): ?[SqlFType] = 
      if (params.length < 1) fail("Expected at least 1 parameter")
      else for {
        in <- sequence(params map (p => tpeOf(p, ct)))
      } yield (in, ((typeOf[String], JdbcTypes.VARCHAR), in.map(_._2).forall(identity)))

    private def castToType(orig: Type, target: Expr) = target match {
      case TypeExpr(d) => d.name match {
        case "date" => ((typeOf[java.sql.Date], JdbcTypes.DATE), true).ok
        case "datetime" => ((typeOf[java.sql.Timestamp], JdbcTypes.TIMESTAMP), true).ok
        case "time" => ((typeOf[java.sql.Time], JdbcTypes.TIME), true).ok
        case "char" => ((typeOf[String], JdbcTypes.CHAR), false).ok
        case "binary" => ((typeOf[String], JdbcTypes.BINARY), false).ok
        case "decimal" => ((typeOf[Double], JdbcTypes.DECIMAL), false).ok
        case "signed" if orig == typeOf[Long] => ((typeOf[Long], JdbcTypes.BIGINT), false).ok
        case "signed" => ((typeOf[Int], JdbcTypes.INTEGER), false).ok
        case "unsigned" if orig == typeOf[Long] => ((typeOf[Long], JdbcTypes.BIGINT), false).ok
        case "unsigned" => ((typeOf[Int], JdbcTypes.INTEGER), false).ok
        case x => fail(s"Unsupported type '$target' in cast operation")
      }
      case e => fail(s"Expected a data type, got '$e'")
    }
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
    lazy val interval = "interval".i ~> intervalAmount ~ timeUnit ^^ { case x ~ _ => const((typeOf[java.util.Date], JdbcTypes.TIMESTAMP), x) }

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

object PostgresqlDialect extends Dialect {
  val parser = new SqlParser {}
  def validator = JdbcValidator

  def typer(schema: Schema, stmt: Statement[Table], dbConfig: DbConfig) = new Typer(schema, stmt, dbConfig) {
    import dsl._

    override def extraScalarFunctions = Map(
        "any"  -> arrayExpr _
      , "some" -> arrayExpr _
      , "all"  -> arrayExpr _
    )

    def arrayExpr(fname: String, params: List[Expr], ct: Option[Term]): ?[SqlFType] = 
      if (params.length != 1) fail("Expected 1 parameter " + params)
      else for {
        (tpe0, opt0) <- tpeOf(params(0), ct)
      } yield (List(((appliedType(typeOf[Seq[_]].typeConstructor, tpe0._1 :: Nil), JdbcTypes.ARRAY), false)), (tpe0, opt0))
  }
}

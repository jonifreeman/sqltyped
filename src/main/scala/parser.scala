package sqltyped

import scala.util.parsing.combinator._
import scala.reflect.runtime.universe.{Type, typeOf}

trait SqlParser extends RegexParsers with Ast.Unresolved {
  import Ast._

  def parse(sql: String): ?[Statement] = parse(stmt, sql) match {
    case Success(r, q)  => Right(r)
    case err: NoSuccess => Left(err.msg)
  }

  lazy val stmt = (unionStmt | selectStmt | insertStmt | updateStmt | deleteStmt | createStmt)

  lazy val selectStmt = select ~ from ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ~ opt(limit) ^^ {
    case s ~ f ~ w ~ g ~ o ~ l => Select(s, f, w, g, o, l)
  }

  lazy val unionStmt = optParens(selectStmt) ~ "union".i ~ optParens(selectStmt) ~ opt(orderBy) ~ opt(limit) ^^ {
    case s1 ~ _ ~ s2 ~ o ~ l => Union(s1, s2, o, l)
  }

  lazy val insertStmt = "insert".i ~ "into".i ~ table ~ opt(colNames) ~ (listValues | selectValues) ^^ {
    case _ ~ _ ~ t ~ cols ~ vals => Insert(t, cols, vals)
  }

  lazy val colNames = "(" ~> repsep(ident, ",") <~ ")"

  lazy val listValues = "values".i ~> "(" ~> repsep(term, ",") <~ ")" ^^ ListedInput.apply

  lazy val selectValues = selectStmt ^^ SelectedInput.apply

  lazy val updateStmt = "update".i ~ repsep(table, ",") ~ "set".i ~ repsep(assignment, ",") ~ opt(where) ~ opt(orderBy) ~ opt(limit) ^^ {
    case _ ~ t ~ _ ~ a ~ w ~ o ~ l => Update(t, a, w, o, l)
  }

  lazy val assignment = column ~ "=" ~ term ^^ { case c ~ _ ~ t => (c, t) }

  lazy val deleteStmt = "delete".i ~ opt(repsep(ident, ",")) ~ from ~ opt(where) ^^ {
    case _ ~ f ~ w => Delete(f, w)
  }

  lazy val createStmt = "create".i ^^^ Create[Option[String]]()

  lazy val select = "select".i ~> repsep((opt("distinct".i) ~> value), ",")

  lazy val from = "from".i ~> rep1sep(join, ",")

  lazy val join = table ~ rep(joinSpec) ^^ { case t ~ j => From(t, j) }

  lazy val joinSpec = opt("left".i | "right".i) ~ opt("inner".i | "outer".i) ~ "join".i ~ table ~ "on".i ~ expr ^^ {
    case side ~ joinType ~ _ ~ table ~ "on" ~ expr => 
      Join(table, expr, side.map(_ + " ").getOrElse("") + joinType.map(_ + " ").getOrElse("") + "join")
  }

  lazy val table = ident ~ opt(opt("as".i) ~> ident) ^^ { case n ~ a => Table(n, a) }

  lazy val where = "where".i ~> expr ^^ Where.apply

  lazy val expr: Parser[Expr] = (predicate | parens)* (
      "and".i ^^^ { (e1: Expr, e2: Expr) => And(e1, e2) } 
    | "or".i  ^^^ { (e1: Expr, e2: Expr) => Or(e1, e2) } 
  )

  lazy val parens: Parser[Expr] = "(" ~> expr  <~ ")"

  lazy val predicate: Parser[Predicate] = (
      term ~ "="  ~ (term | subselect)       ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Eq, rhs) }
    | term ~ "!=" ~ (term | subselect)       ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Neq, rhs) }
    | term ~ "<"  ~ (term | subselect)       ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Lt, rhs) }
    | term ~ ">"  ~ (term | subselect)       ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Gt, rhs) }
    | term ~ "<=" ~ (term | subselect)       ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Le, rhs) }
    | term ~ ">=" ~ (term | subselect)       ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Ge, rhs) }
    | term ~ "in".i ~ subselect              ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, In, rhs) }
    | term ~ "between".i ~ term ~ "and".i ~ term ^^ { case t1 ~ _ ~ t2 ~ _ ~ t3 => Predicate3(t1, Between, t2, t3) }
    | term <~ "is".i ~ "null".i              ^^ { t => Predicate1(t, IsNull) }
    | term <~ "is".i ~ "not".i ~ "null".i    ^^ { t => Predicate1(t, IsNotNull) }
  )

  lazy val subselect = "(" ~> selectStmt <~ ")" ^^ Subselect.apply

  lazy val term = (arith | simpleTerm)

  lazy val simpleTerm = (
      boolean    ^^ constB
    | stringLit  ^^ constS
    | numericLit ^^ (n => if (n.contains(".")) constD(n.toDouble) else constL(n.toLong))
    | extraValues
    | function
    | column
    | "?"        ^^^ Input[Option[String]]()
  )

  lazy val value = (arith | simpleValue) ~ opt(opt("as".i) ~> ident) ^^ {
    case (c@Constant(_, _)) ~ a     => Named("<constant>", a, c)
    case (f@Function(n, _)) ~ a     => Named(n, a, f)
    case (c@Column(n, _)) ~ a       => Named(n, a, c)
    case (c@AllColumns(_)) ~ a      => Named("*", a, c)
    case (e@ArithExpr(_, _, _)) ~ a => Named("<constant>", a, e)
  }

  lazy val simpleValue: Parser[Value] = (
      boolean    ^^ constB
    | stringLit  ^^ constS
    | numericLit ^^ (n => if (n.contains(".")) constD(n.toDouble) else constL(n.toLong))
    | function
    | extraValues
    | column
    | allColumns
  )

  def extraValues: Parser[Value] = failure("no extra values")

  lazy val column = (
      ident ~ "." ~ ident ^^ { case t ~ _ ~ c => col(c, Some(t)) }
    | ident ^^ (c => col(c, None))
  )

  lazy val allColumns = 
    "*" ~ opt("." ~> ident) ^^ { case _ ~ t => AllColumns(t) }

  lazy val function: Parser[Function] = 
    ident ~ "(" ~ repsep(term, ",") ~ ")" ^^ {
      case name ~ _ ~ params ~ _ => Function(name, params)
    }

  lazy val arith: Parser[Value] = (simpleValue | arithParens)* (
      "+" ^^^ { (lhs: Value, rhs: Value) => ArithExpr(lhs, "+", rhs) }
    | "-" ^^^ { (lhs: Value, rhs: Value) => ArithExpr(lhs, "-", rhs) }
    | "*" ^^^ { (lhs: Value, rhs: Value) => ArithExpr(lhs, "*", rhs) }
    | "/" ^^^ { (lhs: Value, rhs: Value) => ArithExpr(lhs, "/", rhs) }
    | "%" ^^^ { (lhs: Value, rhs: Value) => ArithExpr(lhs, "%", rhs) }
  )

  lazy val arithParens = "(" ~> arith <~ ")"

  lazy val boolean = ("true".i ^^^ true | "false".i ^^^ false)

  lazy val orderBy = "order".i ~> "by".i ~> repsep(column, ",") ~ opt("asc".i ^^^ Asc | "desc".i ^^^ Desc) ^^ {
    case cols ~ order => OrderBy(cols, order)
  }

  lazy val groupBy = "group".i ~> "by".i ~> column ~ opt(having) ^^ {
    case col ~ having => GroupBy(col, having)
  }

  lazy val having = "having".i ~> expr ^^ Having.apply

  lazy val limit = "limit".i ~> intOrInput ~ opt("offset".i ~> intOrInput) ^^ {
    case count ~ offset => Limit(count, offset)
  }

  lazy val intOrInput = (
      "?" ^^^ Right(Input[Option[String]]())
    | numericLit ^^ (n => Left(n.toInt))
  )

  def optParens[A](p: Parser[A]): Parser[A] = (
      "(" ~> p <~ ")"
    | p
  )

  val reserved = 
    ("select".i | "delete".i | "insert".i | "update".i | "from".i | "into".i | "where".i | "as".i | 
     "and".i | "or".i | "join".i | "inner".i | "outer".i | "left".i | "right".i | "on".i | "group".i |
     "by".i | "having".i | "limit".i | "offset".i | "order".i | "asc".i | "desc".i | "distinct".i | 
     "is".i | "not".i | "null".i | "between".i | "in".i | "exists".i | "values".i | "create".i | 
     "set".i | "union".i)

  private def col(name: String, table: Option[String]) = Column(name, table)

  def constB(b: Boolean)       = const(typeOf[Boolean], b)
  def constS(s: String)        = const(typeOf[String], s)
  def constD(d: Double)        = const(typeOf[Double], d)
  def constL(l: Long)          = const(typeOf[Long], l)
  def const(tpe: Type, x: Any) = Constant[Option[String]](tpe, x)

  implicit class KeywordOps(kw: String) {
    def i = keyword(kw)
  }

  def keyword(kw: String): Parser[String] = ("(?i)" + kw + "\\b").r

  val ident = not(reserved) ~> "[a-zA-Z][a-zA-Z0-9_-]*".r

  val stringLit = 
    "'" ~ """([^'\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*""".r ~ "'" ^^ { case _ ~ s ~ _ => s }

  val numericLit: Parser[String] = """(\d+(\.\d*)?|\d*\.\d+)""".r
}

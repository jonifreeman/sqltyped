package sqltyped

import scala.util.parsing.combinator._
import scala.reflect.runtime.universe.{Type, typeOf}

object SqlParser extends RegexParsers {
  import Ast._

  def parse(sql: String): Either[String, Statement] = parse(stmt, sql) match {
    case Success(r, q)  => Right(r)
    case err: NoSuccess => Left(err.msg)
  }

  def stmt = (unionStmt | selectStmt | insertStmt | updateStmt | deleteStmt | createStmt)

  def selectStmt = select ~ from ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ~ opt(limit) ^^ {
    case s ~ f ~ w ~ g ~ o ~ l => Select(s, f, w, g, o, l)
  }

  def unionStmt = optParens(selectStmt) ~ "union".i ~ optParens(selectStmt) ~ opt(orderBy) ~ opt(limit) ^^ {
    case s1 ~ _ ~ s2 ~ o ~ l => Union(s1, s2, o, l)
  }

  def insertStmt = "insert".i ~ "into".i ~ table ~ opt(colNames) ~ (listValues | selectValues) ^^ {
    case _ ~ _ ~ t ~ cols ~ vals => Insert(t, cols, vals)
  }

  def colNames = "(" ~> repsep(ident, ",") <~ ")"

  def listValues = "values".i ~> "(" ~> repsep(term, ",") <~ ")" ^^ ListedInput.apply

  def selectValues = selectStmt ^^ SelectedInput.apply

  def updateStmt = "update".i ~ repsep(table, ",") ~ "set".i ~ repsep(assignment, ",") ~ opt(where) ~ opt(orderBy) ~ opt(limit) ^^ {
    case _ ~ t ~ _ ~ a ~ w ~ o ~ l => Update(t, a, w, o, l)
  }

  def assignment = column ~ "=" ~ term ^^ { case c ~ _ ~ t => (c, t) }

  def deleteStmt = "delete".i ~ opt(repsep(ident, ",")) ~ from ~ opt(where) ^^ {
    case _ ~ f ~ w => Delete(f, w)
  }

  def createStmt = "create".i ^^^ Create

  def select: Parser[List[Value]] = "select".i ~> repsep((opt("distinct".i) ~> value), ",")

  def from: Parser[List[From]] = "from".i ~> rep1sep(join, ",")

  def join = table ~ rep(joinSpec) ^^ { case t ~ j => From(t, j) }

  def joinSpec = opt("left".i | "right".i) ~ opt("inner".i | "outer".i) ~ "join".i ~ table ~ "on".i ~ expr ^^ {
    case side ~ joinType ~ _ ~ table ~ "on" ~ expr => 
      Join(table, expr, side.map(_ + " ").getOrElse("") + joinType.map(_ + " ").getOrElse("") + "join")
  }

  def table = ident ~ opt(opt("as".i) ~> ident) ^^ { case n ~ a => Table(n, a) }

  def where = "where".i ~> expr ^^ Where.apply

  def expr: Parser[Expr] = (predicate | parens)* (
      "and" ^^^ { (e1: Expr, e2: Expr) => And(e1, e2) } 
    | "or"  ^^^ { (e1: Expr, e2: Expr) => Or(e1, e2) } 
  )

  def parens: Parser[Expr] = "(" ~> expr  <~ ")"

  def predicate: Parser[Predicate] = (
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

  def subselect = "(" ~> selectStmt <~ ")" ^^ Subselect.apply

  def term = (arith | simpleTerm)

  def simpleTerm: Parser[Term] = (
      boolean    ^^ (b => Constant(typeOf[Boolean], b))
    | stringLit  ^^ (s => Constant(typeOf[String], s))
    | numericLit ^^ (n => if (n.contains(".")) Constant(typeOf[Double], n.toDouble) else Constant(typeOf[Long], n.toInt))
    | function
    | column
    | "?"        ^^^ Input
  )

  def value = (arith | simpleValue)

  def simpleValue: Parser[Value] = (
      boolean    ^^ (b => Constant(typeOf[Boolean], b))
    | stringLit  ^^ (s => Constant(typeOf[String], s))
    | numericLit ^^ (n => if (n.contains(".")) Constant(typeOf[Double], n.toDouble) else Constant(typeOf[Long], n.toInt))
    | function
    | column
    | allColumns
  )

  def column = (
      ident ~ "." ~ ident ~ "as".i ~ ident ^^ { case t ~ _ ~ c ~ _ ~ a => Column(c, Some(t), Some(a)) }
    | ident ~ "." ~ ident ^^ { case t ~ _ ~ c => Column(c, Some(t), None) }
    | ident ~ "as".i ~ ident ^^ { case c ~ _ ~ a => Column(c, None, Some(a)) }
    | ident ^^ (c => Column(c, None, None))
  )

  def allColumns = "*" ~ opt("." ~> ident) ^^ { case _ ~ t => AllColumns(t) }

  def function: Parser[Function] = 
    ident ~ "(" ~ repsep(term, ",") ~ ")" ~ opt("as".i ~> ident) ^^ {
      case name ~ _ ~ params ~ _ ~ alias => Function(name, params, alias)
    }

  def arith: Parser[Value] = (simpleValue | arithParens)* (
      "+" ^^^ { (lhs: Value, rhs: Value) => ArithExpr(lhs, "+", rhs) }
    | "-" ^^^ { (lhs: Value, rhs: Value) => ArithExpr(lhs, "-", rhs) }
    | "*" ^^^ { (lhs: Value, rhs: Value) => ArithExpr(lhs, "*", rhs) }
    | "/" ^^^ { (lhs: Value, rhs: Value) => ArithExpr(lhs, "/", rhs) }
    | "%" ^^^ { (lhs: Value, rhs: Value) => ArithExpr(lhs, "%", rhs) }
  )

  def arithParens = "(" ~> arith <~ ")"

  def boolean = ("true".i ^^^ true | "false".i ^^^ false)

  def orderBy = "order".i ~> "by".i ~> repsep(column, ",") ~ opt("asc".i ^^^ Asc | "desc".i ^^^ Desc) ^^ {
    case cols ~ order => OrderBy(cols, order)
  }

  def groupBy = "group".i ~> "by".i ~> column ~ opt(having) ^^ {
    case col ~ having => GroupBy(col, having)
  }

  def having = "having".i ~> expr ^^ Having.apply

  def limit = "limit".i ~> intOrInput ~ opt("offset".i ~> intOrInput) ^^ {
    case count ~ offset => Limit(count, offset)
  }

  def intOrInput = (
      "?" ^^^ Right(Input)
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

  private implicit class KeywordOps(kw: String) {
    def i = keyword(kw)
  }

  def keyword(kw: String): Parser[String] = ("(?i)" + kw + "\\b").r

  val ident = not(reserved) ~> "[a-zA-Z][a-zA-Z0-9_-]*".r

  val stringLit = 
    "'" ~ """([^'\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*""".r ~ "'" ^^ { case _ ~ s ~ _ => s }

  val numericLit: Parser[String] = """(\d+(\.\d*)?|\d*\.\d+)""".r
}

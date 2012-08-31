package sqltyped

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import scala.reflect.runtime.universe.{Type, typeOf}

object SqlParser extends StandardTokenParsers {
  import Ast._

  lexical.delimiters ++= List("(", ")", ",", " ", "=", ">", "<", ">=", "<=", "?", "!=", ".")
  lexical.reserved += ("select", "from", "where", "as", "and", "or", "join", "inner", "outer", "left", 
                       "right", "on", "group", "by", "having", "limit", "offset", "order", "asc", 
                       "desc", "distinct", "is", "not", "null", "between")

  def parse(sql: String): Either[String, Statement] = selectStmt(new lexical.Scanner(sql)) match {
    case Success(r, q)  => Right(r)
    case err: NoSuccess => Left(err.msg)
  }

  def selectStmt = select ~ from ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ~ opt(limit) ^^ {
    case s ~ f ~ w ~ g ~ o ~ l => Select(s, f, w, g, o, l)
  }

  def select: Parser[List[Value]] = "select" ~> repsep((opt("distinct") ~> value), ",")

  def from: Parser[List[From]] = "from" ~> rep1sep(join, ",")

  def join = table ~ rep(joinSpec) ^^ { case t ~ j => From(t, j) }

  def joinSpec = opt("left" | "right") ~ opt("inner" | "outer") ~ "join" ~ table ~ "on" ~ expr ^^ {
    case side ~ joinType ~ _ ~ table ~ "on" ~ expr => 
      Join(table, expr, side.map(_ + " ").getOrElse("") + joinType.map(_ + " ").getOrElse("") + "join")
  }

  def table = (
      ident ~ "as" ~ ident  ^^ { case n ~ _ ~ a => Table(n, Some(a)) }
    | ident ~ ident         ^^ { case n ~ a => Table(n, Some(a)) }
    | ident                 ^^ { case n => Table(n, None) }
  )

  def where = "where" ~> expr ^^ Where.apply

  def expr: Parser[Expr] = (subselect | predicate | parens)* (
      "and" ^^^ { (e1: Expr, e2: Expr) => And(e1, e2) } 
    | "or"  ^^^ { (e1: Expr, e2: Expr) => Or(e1, e2) } 
  )

  def parens: Parser[Expr] = "(" ~> expr  <~ ")"

  def subselect = selectStmt ^^ Subselect.apply

  def predicate: Parser[Predicate] = (
      term ~ "=" ~ term                      ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Eq, rhs) }
    | term ~ "!=" ~ term                     ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Neq, rhs) }
    | term ~ "<" ~ term                      ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Lt, rhs) }
    | term ~ ">" ~ term                      ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Gt, rhs) }
    | term ~ "<=" ~ term                     ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Le, rhs) }
    | term ~ ">=" ~ term                     ^^ { case lhs ~ _ ~ rhs => Predicate2(lhs, Ge, rhs) }
    | term ~ "between" ~ term ~ "and" ~ term ^^ { case t1 ~ _ ~ t2 ~ _ ~ t3 => Predicate3(t1, Between, t2, t3) }
    | term <~ "is" ~ "null"                  ^^ { t => Predicate1(t, IsNull) }
    | term <~ "is" ~ "not" ~ "null"          ^^ { t => Predicate1(t, IsNotNull) }
  )

  def term: Parser[Term] = (
      boolean    ^^ (b => Constant(typeOf[Boolean], b))
    | stringLit  ^^ (s => Constant(typeOf[String], s))
    | numericLit ^^ (n => if (n.contains(".")) Constant(typeOf[Double], n.toDouble) else Constant(typeOf[Long], n.toInt))
    | function
    | column
    | chr('?')   ^^^ Input
  )

  def value: Parser[Value] = (
      boolean    ^^ (b => Constant(typeOf[Boolean], b))
    | stringLit  ^^ (s => Constant(typeOf[String], s))
    | numericLit ^^ (n => if (n.contains(".")) Constant(typeOf[Double], n.toDouble) else Constant(typeOf[Long], n.toInt))
    | function
    | column
  )

  def column = (
      ident ~ "." ~ ident ~ "as" ~ ident ^^ { case t ~ _ ~ c ~ _ ~ a => Column(c, Some(t), Some(a)) }
    | ident ~ "." ~ ident ^^ { case t ~ _ ~ c => Column(c, Some(t), None) }
    | ident ~ "as" ~ ident ^^ { case c ~ _ ~ a => Column(c, None, Some(a)) }
    | ident ^^ (c => Column(c, None, None))
  )

  def function: Parser[Function] = 
    ident ~ "(" ~ repsep(term, ",") ~ ")" ~ opt("as" ~> ident) ^^ {
      case name ~ _ ~ params ~ _ ~ alias => Function(name, params, alias)
    }

  def boolean = ("true" ^^^ true | "false" ^^^ false)

  def orderBy = "order" ~> "by" ~> repsep(column, ",") ~ opt("asc" ^^^ Asc | "desc" ^^^ Desc) ^^ {
    case cols ~ order => OrderBy(cols, order)
  }

  def groupBy = "group" ~> "by" ~> column ~ opt(having) ^^ {
    case col ~ having => GroupBy(col, having)
  }

  def having = "having" ~> expr ^^ Having.apply

  def limit = "limit" ~> intOrInput ~ opt("offset" ~> intOrInput) ^^ {
    case count ~ offset => Limit(count, offset)
  }

  def intOrInput = (
      chr('?') ^^^ Right(Input)
    | numericLit ^^ (n => Left(n.toInt))
  )

  def chr(c: Char) = elem("", _.chars == c.toString)
}

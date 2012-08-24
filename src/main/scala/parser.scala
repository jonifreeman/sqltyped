package sqltyped

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import scala.reflect.runtime.universe.{Type, typeOf}

object SqlParser extends StandardTokenParsers {
  import Ast._

  lexical.delimiters ++= List("(", ")", ",", " ", "=", ">", "<", ">=", "<=", "?", "!=", ".")
  lexical.reserved += ("select", "from", "where", "as", "and", "or", "join", "inner", "outer", "left", 
                       "right", "on", "group", "by", "having", "limit", "offset")

  def parse(sql: String): Either[String, Statement] = selectStmt(new lexical.Scanner(sql)) match {
    case Success(r, q)  => Right(r)
    case err: NoSuccess => Left(err.msg)
  }

  def selectStmt = select ~ from ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ~ opt(limit) ^^ {
    case s ~ f ~ w ~ g ~ o ~ l => Select(s, f, w, g, o, l) // FIXME cleanup
  }

  def select: Parser[List[Value]] = "select" ~> repsep(value, ",")

  def from: Parser[List[From]] = "from" ~> rep1sep(join, ",")

  def join = table ~ opt(joinSpec) ~ repsep(joiningTable, joinSpec) ^^ { case t ~ _ ~ j => From(t, j) }

  def joinSpec = opt("left" | "right") ~> opt("inner" | "outer") ~> "join"
 
  def joiningTable = table ~ "on" ~ expr ^^ {
    case table ~ "on" ~ expr => Join(table, expr)
  }

  def table = (
      ident ~ "as" ~ ident  ^^ { case n ~ _ ~ a => Table(n, Some(a)) }
    | ident ~ ident         ^^ { case n ~ a => Table(n, Some(a)) }
    | ident                 ^^ { case n => Table(n, None) }
  )

  def where = "where" ~> expr ^^ Where.apply

  def expr: Parser[Expr] = (
      predicate
    | expr ~ "and" ~ expr ^^ { case e1 ~ _ ~ e2 => And(e1, e2) } // FIXME can this be cleaned?
    | expr ~ "or" ~ expr  ^^ { case e1 ~ _ ~ e2 => Or(e1, e2) }
  )

  def predicate: Parser[Predicate] = (
      term ~ "=" ~ term  ^^ { case lhs ~ _ ~ rhs => Predicate(lhs, Eq, rhs) }
    | term ~ "!=" ~ term ^^ { case lhs ~ _ ~ rhs => Predicate(lhs, Neq, rhs) }
    | term ~ "<" ~ term  ^^ { case lhs ~ _ ~ rhs => Predicate(lhs, Lt, rhs) }
    | term ~ ">" ~ term  ^^ { case lhs ~ _ ~ rhs => Predicate(lhs, Gt, rhs) }
    | term ~ "<=" ~ term ^^ { case lhs ~ _ ~ rhs => Predicate(lhs, Le, rhs) }
    | term ~ ">=" ~ term ^^ { case lhs ~ _ ~ rhs => Predicate(lhs, Ge, rhs) }
  )

  def term: Parser[Term] = (
      boolean    ^^^ Constant(typeOf[Boolean])
    | stringLit  ^^^ Constant(typeOf[String])
    | numericLit ^^  (n => if (n.contains(".")) Constant(typeOf[Double]) else Constant(typeOf[Long]))
    | function
    | column
    | chr('?')   ^^^ Input
  )

  def value: Parser[Value] = (
      boolean    ^^^ Constant(typeOf[Boolean])
    | stringLit  ^^^ Constant(typeOf[String])
    | numericLit ^^  (n => if (n.contains(".")) Constant(typeOf[Double]) else Constant(typeOf[Long]))
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

  def limit = "limit" ~> numericLit ~ opt("offset" ~> numericLit) ^^ {
    case count ~ offset => Limit(count.toInt, offset.map(_.toInt))
  }

  def chr(c: Char) = elem("", _.chars == c.toString)
}


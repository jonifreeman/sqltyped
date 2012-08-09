package sqltyped

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._

case class Column(table: String, name: String)

// FIXME implement full SQL
object SqlParser extends JavaTokenParsers {
  def parse(sql: String): Either[String, List[Column]] = parseAll(selectStmt, sql) match {
    case Success(r, q)  => Right(r)
    case err: NoSuccess => Left(err.msg)
  }

  def selectStmt = select ~ from ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ^^ {
    case select ~ from ~ where ~ groupBy ~ orderBy => mkColumns(select, from)
  }

  def select: Parser[List[String]] = "select" ~> repsep(ident, ",")

  def from: Parser[List[String]] = "from" ~> rep1sep(ident, ",")

  def where = "where" ~> rep1sep(predicate, ("and" | "or"))
  
  def predicate = (
      ident ~ "=" ~ boolean
    | ident ~ "=" ~ stringLiteral
    | ident ~ "=" ~ wholeNumber
  )

  def boolean = ("true" ^^^ true | "false" ^^^ false)

  def orderBy = "order" ~> "by" ~> ident ~ opt("asc" | "desc")

  def groupBy = "group" ~> "by" ~> ident

  // FIXME supports one table only
  private def mkColumns(select: List[String], from: List[String]) =
    select.map(Column(from.head, _))
}


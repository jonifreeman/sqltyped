package sqltyped

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._

case class Select(in: List[Column], out: List[Column])
case class Column(table: String, name: String)

// FIXME implement full SQL
object SqlParser extends StandardTokenParsers {
  lexical.delimiters ++= List("(", ")", ",", " ", "=", ">", "<", "?", ".")
  lexical.reserved += ("select", "from", "where", "as", "and", "or")

  case class Table(name: String, alias: Option[String])
  case class ColumnRef(name: String, tableRef: Option[String])

  def parse(sql: String): Either[String, Select] = selectStmt(new lexical.Scanner(sql)) match {
    case Success(r, q)  => Right(r)
    case err: NoSuccess => Left(err.msg)
  }

  def selectStmt = select ~ from ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ^^ {
    case select ~ from ~ where ~ groupBy ~ orderBy => 
      Select(mkColumns(where.getOrElse(Nil), from), mkColumns(select, from))
  }

  def select: Parser[List[ColumnRef]] = "select" ~> repsep(column, ",")

  def from: Parser[List[Table]] = "from" ~> rep1sep(table, ",")

  def table = (
      ident ~ "as" ~ ident  ^^ { case n ~ _ ~ a => Table(n, Some(a)) }
    | ident ~ ident         ^^ { case n ~ a => Table(n, Some(a)) }
    | ident                 ^^ { case n => Table(n, None) }
  )

  def where = "where" ~> rep1sep(predicate, ("and" | "or")) ^^ { 
    case xs => xs collect { case Some(x) => x }
  }

  def predicate = (
      column ~ "=" ~ boolean    ^^ (_ => None)
    | column ~ "=" ~ stringLit  ^^ (_ => None)
    | column ~ "=" ~ numericLit ^^ (_ => None)
    | column ~ "=" ~ column     ^^ (_ => None)
    | column <~ "=" ~ chr('?')  ^^ Some.apply
    | column ~ "<" ~ numericLit ^^ (_ => None)
    | column ~ "<" ~ column     ^^ (_ => None)
    | column <~ "<" ~ chr('?')  ^^ Some.apply
    | column ~ ">" ~ numericLit ^^ (_ => None)
    | column ~ ">" ~ column     ^^ (_ => None)
    | column <~ ">" ~ chr('?')  ^^ Some.apply
  )

  def column = (
      ident ~ "." ~ ident ^^ { case t ~ _ ~ c => ColumnRef(c, Some(t)) }
    | ident ^^ (c => ColumnRef(c, None))
  )
  
  def boolean = ("true" ^^^ true | "false" ^^^ false)

  def orderBy = "order" ~> "by" ~> ident ~ opt("asc" | "desc")

  def groupBy = "group" ~> "by" ~> ident

  def chr(c: Char) = elem("", _.chars == c.toString)

  // FIXME improve error handling
  private def mkColumns(select: List[ColumnRef], from: List[Table]) = {
    def findTable(c: ColumnRef) = from.find { t => 
      (c.tableRef, t.alias) match {
        case (Some(ref), None) => t.name == ref
        case (Some(ref), Some(a)) => t.name == ref || a == ref
        case (None, _) => true
      }
    } getOrElse(sys.error("Column references invalid table " + c))

    select.map(c => Column(findTable(c).name, c.name))
  }
}


package sqltyped

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._

case class Select(in: List[Column], out: List[Column])
case class Column(table: String, name: String, alias: Option[String] = None)

// FIXME implement full SQL
object SqlParser extends StandardTokenParsers {
  lexical.delimiters ++= List("(", ")", ",", " ", "=", ">", "<", "?", "!=", ".")
  lexical.reserved += ("select", "from", "where", "as", "and", "or", "join", "inner", "outer", "left", 
                       "right", "on")

  case class Table(name: String, alias: Option[String])
  case class ColumnRef(name: String, tableRef: Option[String], alias: Option[String])

  def parse(sql: String): Either[String, Select] = selectStmt(new lexical.Scanner(sql)) match {
    case Success(r, q)  => Right(r)
    case err: NoSuccess => Left(err.msg)
  }

  def selectStmt = select ~ from ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ^^ {
    case select ~ from ~ where ~ groupBy ~ orderBy => 
      Select(mkColumns(from._2 ::: where.getOrElse(Nil), from._1), mkColumns(select, from._1))
  }

  def select: Parser[List[ColumnRef]] = "select" ~> repsep(column, ",")

  def from: Parser[(List[Table], List[ColumnRef])] = "from" ~> rep1sep(join, ",") ^^ {
    defs => defs.unzip match { case (tables, columns) => (tables.flatten, columns.flatten) }
  }

  def join = table ~ opt(joinSpec) ~ repsep(joiningTable, joinSpec) ^^ {
    case table ~ _ ~ joins => (table :: joins.map(_._1), joins.flatMap(_._2))
  }

  def joinSpec = opt("left" | "right") ~> opt("inner" | "outer") ~> "join"
 
  def joiningTable = table ~ "on" ~ rep1sep(predicate, ("and" | "or")) ^^ {
    case table ~ "on" ~ predicates => (table, predicates collect { case Some(c) => c })
  }

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
    | column ~ "!=" ~ boolean    ^^ (_ => None)
    | column ~ "!=" ~ stringLit  ^^ (_ => None)
    | column ~ "!=" ~ numericLit ^^ (_ => None)
    | column ~ "!=" ~ column     ^^ (_ => None)
    | column <~ "!=" ~ chr('?')  ^^ Some.apply
    | column ~ "<" ~ numericLit ^^ (_ => None)
    | column ~ "<" ~ column     ^^ (_ => None)
    | column <~ "<" ~ chr('?')  ^^ Some.apply
    | column ~ ">" ~ numericLit ^^ (_ => None)
    | column ~ ">" ~ column     ^^ (_ => None)
    | column <~ ">" ~ chr('?')  ^^ Some.apply
  )

  def column = (
      ident ~ "." ~ ident ~ "as" ~ ident ^^ { case t ~ _ ~ c ~ _ ~ a => ColumnRef(c, Some(t), Some(a)) }
    | ident ~ "." ~ ident ^^ { case t ~ _ ~ c => ColumnRef(c, Some(t), None) }
    | ident ~ "as" ~ ident ^^ { case c ~ _ ~ a => ColumnRef(c, None, Some(a)) }
    | ident ^^ (c => ColumnRef(c, None, None))
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
    } getOrElse(sys.error("Column references invalid table " + c + ", tables " + from))

    select.map(c => Column(findTable(c).name, c.name, c.alias))
  }
}


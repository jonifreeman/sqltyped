package sqltyped

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import scala.reflect.runtime.universe.{Type, typeOf}

case class Statement(in: List[Expr], out: List[Expr])

sealed trait Expr { def name: String }
case class Constant(tpe: Type) extends Expr {
  def name = "<constant>"
}
case class Column(table: String, cname: String, alias: Option[String] = None) extends Expr {
  def name = alias getOrElse cname
}
case class Function(fname: String, params: List[Expr], alias: Option[String] = None) extends Expr {
  def name = alias getOrElse fname
}

// FIXME implement full SQL
object SqlParser extends StandardTokenParsers {
  lexical.delimiters ++= List("(", ")", ",", " ", "=", ">", "<", ">=", "<=", "?", "!=", ".")
  lexical.reserved += ("select", "from", "where", "as", "and", "or", "join", "inner", "outer", "left", 
                       "right", "on", "group", "by", "having")

  case class Table(name: String, alias: Option[String])
  sealed trait ExprRef
  case class ConstantRef(tpe: Type) extends ExprRef
  case class ColumnRef(name: String, tableRef: Option[String], alias: Option[String]) extends ExprRef
  case class FunctionRef(name: String, params: List[ExprRef], alias: Option[String]) extends ExprRef

  def parse(sql: String): Either[String, Statement] = selectStmt(new lexical.Scanner(sql)) match {
    case Success(r, q)  => Right(r)
    case err: NoSuccess => Left(err.msg)
  }

  def selectStmt = select ~ from ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ^^ {
    case select ~ from ~ where ~ groupBy ~ orderBy => 
      Statement(mkColumns(from._2 ::: where.getOrElse(Nil) ::: groupBy.getOrElse(Nil), from._1), 
                mkColumns(select, from._1))
  }

  def select: Parser[List[ExprRef]] = "select" ~> repsep(expr, ",")

  def from: Parser[(List[Table], List[ExprRef])] = "from" ~> rep1sep(join, ",") ^^ {
    defs => defs.unzip match { case (tables, exprs) => (tables.flatten, exprs.flatten) }
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

  def expr = function | column

  def predicate = (
      expr ~ "=" ~ boolean     ^^^ None
    | expr ~ "=" ~ stringLit   ^^^ None
    | expr ~ "=" ~ numericLit  ^^^ None
    | expr ~ "=" ~ expr        ^^^ None
    | expr <~ "=" ~ chr('?')   ^^  Some.apply
    | expr ~ "!=" ~ boolean    ^^^ None
    | expr ~ "!=" ~ stringLit  ^^^ None
    | expr ~ "!=" ~ numericLit ^^^ None
    | expr ~ "!=" ~ expr       ^^^ None
    | expr <~ "!=" ~ chr('?')  ^^  Some.apply
    | expr ~ "<" ~ numericLit  ^^^ None
    | expr ~ "<" ~ expr        ^^^ None
    | expr <~ "<" ~ chr('?')   ^^  Some.apply
    | expr ~ "<=" ~ numericLit  ^^^ None
    | expr ~ "<=" ~ expr        ^^^ None
    | expr <~ "<=" ~ chr('?')   ^^  Some.apply
    | expr ~ ">" ~ numericLit  ^^^ None
    | expr ~ ">" ~ expr        ^^^ None
    | expr <~ ">" ~ chr('?')   ^^  Some.apply
    | expr ~ ">=" ~ numericLit  ^^^ None
    | expr ~ ">=" ~ expr        ^^^ None
    | expr <~ ">=" ~ chr('?')   ^^  Some.apply
  )

  def column = (
      ident ~ "." ~ ident ~ "as" ~ ident ^^ { case t ~ _ ~ c ~ _ ~ a => ColumnRef(c, Some(t), Some(a)) }
    | ident ~ "." ~ ident ^^ { case t ~ _ ~ c => ColumnRef(c, Some(t), None) }
    | ident ~ "as" ~ ident ^^ { case c ~ _ ~ a => ColumnRef(c, None, Some(a)) }
    | ident ^^ (c => ColumnRef(c, None, None))
  )

  def function: Parser[FunctionRef] = 
    ident ~ "(" ~ repsep(functionArg, ",") ~ ")" ~ opt("as" ~> ident) ^^ {
      case name ~ _ ~ params ~ _ ~ alias => FunctionRef(name, params, alias)
    }

  def functionArg = (
      ident ~ "." ~ ident ^^ { case t ~ _ ~ c => ColumnRef(c, Some(t), None) }
    | ident               ^^ (c => ColumnRef(c, None, None))
    | function 
    | boolean             ^^^ ConstantRef(typeOf[Boolean])
    | stringLit           ^^^ ConstantRef(typeOf[String])
    | numericLit          ^^ (n => if (n.contains(".")) ConstantRef(typeOf[Double]) 
                                   else ConstantRef(typeOf[Long]))
  )

  def boolean = ("true" ^^^ true | "false" ^^^ false)

  def orderBy = "order" ~> "by" ~> ident ~ opt("asc" | "desc")

  def groupBy: Parser[List[ExprRef]] = "group" ~> "by" ~> ident ~> opt(having) ^^ {
    exprs => exprs.getOrElse(Nil)
  }

  def having = "having" ~> rep1sep(predicate, ("and" | "or")) ^^ { 
    case xs => xs collect { case Some(x) => x }
  }

  def chr(c: Char) = elem("", _.chars == c.toString)

  // FIXME improve error handling
  private def mkColumns(exprs: List[ExprRef], from: List[Table]) = {
    def findTable(c: ColumnRef) = from.find { t => 
      (c.tableRef, t.alias) match {
        case (Some(ref), None) => t.name == ref
        case (Some(ref), Some(a)) => t.name == ref || a == ref
        case (None, _) => true
      }
    } getOrElse sys.error("Column references invalid table " + c + ", tables " + from)

    def refToExpr(ref: ExprRef): Expr = ref match {
      case c@ColumnRef(n, _, a)      => Column(findTable(c).name, n, a)
      case FunctionRef(n, params, a) => Function(n, params map refToExpr, a)
      case ConstantRef(t)            => Constant(t)
    }

    exprs map refToExpr collect { 
      case e: Column => e
      case e: Function => e
    }
  }
}


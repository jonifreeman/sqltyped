package sqltyped

import scala.util.parsing.combinator._
import scala.reflect.runtime.universe.{Type, typeOf}

trait SqlParser extends RegexParsers with Ast.Unresolved with PackratParsers {
  import Ast._

  def parse(sql: String): ?[Statement] = parseAll(stmt, sql) match {
    case Success(r, q)  => Right(r)
    case err: NoSuccess => Left(err.msg)
  }

  lazy val stmt = (unionStmt | selectStmt | insertStmt | updateStmt | deleteStmt | createStmt)

  lazy val selectStmt = select ~ from ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ~ opt(limit) <~ opt("for".i ~ "update".i) ^^ {
    case s ~ f ~ w ~ g ~ o ~ l => Select(s, f, w, g, o, l)
  }

  lazy val unionStmt = optParens(selectStmt) ~ "union".i ~ optParens(selectStmt) ~ opt(orderBy) ~ opt(limit) ^^ {
    case s1 ~ _ ~ s2 ~ o ~ l => Union(s1, s2, o, l)
  }

  lazy val insertSyntax = insert ~> "into".i ~> table ~ opt(colNames) ~ (listValues | selectValues)

  lazy val insertStmt: Parser[Statement] = insertSyntax ^^ {
    case t ~ cols ~ vals => Insert(t, cols, vals)
  }

  lazy val colNames = "(" ~> repsep(ident, ",") <~ ")"

  lazy val listValues = "values".i ~> "(" ~> repsep(term, ",") <~ ")" ^^ ListedInput.apply

  lazy val selectValues = selectStmt ^^ SelectedInput.apply

  lazy val updateStmt = update ~ rep1sep(table, ",") ~ "set".i ~ rep1sep(assignment, ",") ~ opt(where) ~ opt(orderBy) ~ opt(limit) ^^ {
    case _ ~ t ~ _ ~ a ~ w ~ o ~ l => Update(t, a, w, o, l)
  }

  def insert = "insert".i
  def update = "update".i

  lazy val assignment = column ~ "=" ~ term ^^ { case c ~ _ ~ t => (c, t) }

  lazy val deleteStmt = "delete".i ~ opt(repsep(ident, ",")) ~ from ~ opt(where) ^^ {
    case _ ~ f ~ w => Delete(f, w)
  }

  lazy val createStmt = "create".i ^^^ Create[Option[String]]()

  lazy val select = "select".i ~> repsep((opt("distinct".i) ~> named), ",")

  lazy val from = "from".i ~> rep1sep(join, ",")

  lazy val join = table ~ rep(joinSpec) ^^ { case t ~ j => From(t, j) }

  lazy val joinSpec = opt("left".i | "right".i) ~ opt("inner".i | "outer".i) ~ "join".i ~ table ~ "on".i ~ expr ^^ {
    case side ~ joinType ~ _ ~ table ~ "on" ~ expr => 
      Join(table, expr, side.map(_ + " ").getOrElse("") + joinType.map(_ + " ").getOrElse("") + "join")
  }

  lazy val table = ident ~ opt(opt("as".i) ~> ident) ^^ { case n ~ a => Table(n, a) }

  lazy val where = "where".i ~> expr ^^ Where.apply

  lazy val expr: PackratParser[Expr] = (comparisonTerm | parens)* (
      "and".i ^^^ { (e1: Expr, e2: Expr) => And(e1, e2) } 
    | "or".i  ^^^ { (e1: Expr, e2: Expr) => Or(e1, e2) } 
  )

  lazy val parens: PackratParser[Expr] = "(" ~> expr  <~ ")"

  lazy val comparisonTerm  = comparison(subselect)
  lazy val comparisonValue = comparison(failure("subselect not allowed here"))

  def comparison(sub: PackratParser[Term]): PackratParser[Comparison] = (
      term ~ "="  ~ (term | sub)          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Eq, rhs) }
    | term ~ "!=" ~ (term | sub)          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Neq, rhs) }
    | term ~ "<"  ~ (term | sub)          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Lt, rhs) }
    | term ~ ">"  ~ (term | sub)          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Gt, rhs) }
    | term ~ "<=" ~ (term | sub)          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Le, rhs) }
    | term ~ ">=" ~ (term | sub)          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Ge, rhs) }
    | term ~ "like".i ~ (term | sub)      ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Like, rhs) }
    | term ~ "in".i ~ (terms | sub)       ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, In, rhs) }
    | term ~ "between".i ~ term ~ "and".i ~ term ^^ { case t1 ~ _ ~ t2 ~ _ ~ t3 => Comparison3(t1, Between, t2, t3) }
    | term <~ "is".i ~ "null".i           ^^ { t => Comparison1(t, IsNull) }
    | term <~ "is".i ~ "not".i ~ "null".i ^^ { t => Comparison1(t, IsNotNull) }
    | "exists".i ~> sub                   ^^ { t => Comparison1(t, Exists) }
    | "not" ~> "exists".i ~> sub          ^^ { t => Comparison1(t, NotExists) }
  )

  lazy val subselect = "(" ~> selectStmt <~ ")" ^^ Subselect.apply

  lazy val term = (arith | simpleTerm)

  lazy val terms: PackratParser[Term] = "(" ~> repsep(term, ",") <~ ")" ^^ TermList.apply

  lazy val simpleTerm = (
      boolean    ^^ constB
    | nullLit    ^^^ constNull
    | stringLit  ^^ constS
    | numericLit ^^ (n => if (n.contains(".")) constD(n.toDouble) else constL(n.toLong))
    | extraTerms
    | function
    | column
    | allColumns
    | "?"        ^^^ Input[Option[String]]()
  )

  lazy val named = (comparisonValue | arith | simpleTerm) ~ opt(opt("as".i) ~> ident) ^^ {
    case (c@Constant(_, _)) ~ a          => Named("<constant>", a, c)
    case (f@Function(n, _)) ~ a          => Named(n, a, f)
    case (c@Column(n, _)) ~ a            => Named(n, a, c)
    case (c@AllColumns(_)) ~ a           => Named("*", a, c)
    case (e@ArithExpr(_, _, _)) ~ a      => Named("<constant>", a, e)
    case (c@Comparison1(_, _)) ~ a       => Named("<constant>", a, c)
    case (c@Comparison2(_, _, _)) ~ a    => Named("<constant>", a, c)
    case (c@Comparison3(_, _, _, _)) ~ a => Named("<constant>", a, c)
  }

  def extraTerms: PackratParser[Term] = failure("expected a term")

  lazy val column = (
      ident ~ "." ~ ident ^^ { case t ~ _ ~ c => col(c, Some(t)) }
    | ident ^^ (c => col(c, None))
  )

  lazy val allColumns = 
    "*" ~ opt("." ~> ident) ^^ { case _ ~ t => AllColumns(t) }

  lazy val functionArg: PackratParser[Expr] = (expr | term ^^ SimpleExpr.apply)

  lazy val function = (prefixFunction | infixFunction)

  lazy val prefixFunction: PackratParser[Function] = 
    ident ~ "(" ~ repsep(functionArg, ",") ~ ")" ^^ {
      case name ~ _ ~ params ~ _ => Function(name, params)
    }

  lazy val infixFunction: PackratParser[Function] = (
      functionArg ~ "|" ~ functionArg
    | functionArg ~ "&" ~ functionArg
    | functionArg ~ "^" ~ functionArg
    | functionArg ~ "<<" ~ functionArg
    | functionArg ~ ">>" ~ functionArg
  ) ^^ {
    case lhs ~ name ~ rhs => Function(name, List(lhs, rhs))
  }

  lazy val arith: PackratParser[Term] = (simpleTerm | arithParens)* (
      "+" ^^^ { (lhs: Term, rhs: Term) => ArithExpr(lhs, "+", rhs) }
    | "-" ^^^ { (lhs: Term, rhs: Term) => ArithExpr(lhs, "-", rhs) }
    | "*" ^^^ { (lhs: Term, rhs: Term) => ArithExpr(lhs, "*", rhs) }
    | "/" ^^^ { (lhs: Term, rhs: Term) => ArithExpr(lhs, "/", rhs) }
    | "%" ^^^ { (lhs: Term, rhs: Term) => ArithExpr(lhs, "%", rhs) }
  )

  lazy val arithParens = "(" ~> arith <~ ")"

  lazy val boolean = ("true".i ^^^ true | "false".i ^^^ false)

  lazy val nullLit = "null".i

  lazy val orderBy = "order".i ~> "by".i ~> rep1sep(column, ",") ~ opt("asc".i ^^^ Asc | "desc".i ^^^ Desc) ^^ {
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

  def optParens[A](p: PackratParser[A]): PackratParser[A] = (
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
  def constNull                = const(typeOf[AnyRef], null)
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

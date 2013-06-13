package sqltyped

import Ast._
import NumOfResults._

class Analyzer(typer: Typer) extends Ast.Resolved {
  def refine(stmt: Statement, typed: TypedStatement): ?[TypedStatement] = 
    analyzeSelection(stmt, typed.copy(numOfResults = analyzeResults(stmt, typed))).ok

  /**
   * Statement returns 0 - 1 rows if,
   * 
   * - It is SQL insert
   * - It has no joins and it contains only 'and' expressions in its where clause and at least one of
   *   those targets unique constraint with '=' operator
   * - It has LIMIT 1 clause
   *
   * Statement returns 1 row if,
   *
   * - The projection contains aggregate function and there's no group by
   */
  private def analyzeResults(stmt: Statement, typed: TypedStatement): NumOfResults = {
    import scala.math.Ordering.Implicits._

    def inWhereClause(s: Select, cols: List[Column]) = {
      def inExpr(e: Expr, col: Column): Boolean = e match {
        // note, column comparision works since we only examine statements with one table
        case Comparison1(_, _)                     => false
        case Comparison2(Column(n, _), Eq, _)      => col.name == n 
        case Comparison2(_, Eq, Column(n, _))      => col.name == n
        case Comparison2(_, _, _)                  => false
        case Comparison3(_, _, _, _)               => false
        case And(e1, e2)                           => inExpr(e1, col) || inExpr(e2, col)
        case Or(e1, e2)                            => inExpr(e1, col) || inExpr(e2, col)
        case Not(e)                                => inExpr(e, col)
      }
      s.where.map(w => cols.map(col => inExpr(w.expr, col)).forall(identity)).getOrElse(false)
    }

    def hasLimit1(s: Select) = s.limit.map {
      _.count match {
        case Left(x) => x == 1
        case _ => false
      }
    } getOrElse false

    def hasAggregate(projection: List[Named]) = 
      projection collect { 
        case Named(_, _, Function(n, _)) => n 
        case Named(_, _, Comparison2(Function(n, _), _, _)) => n
        case Named(_, _, Comparison2(_, _, Function(n, _))) => n
      } exists typer.isAggregate

    def hasJoin(t: TableReference) = t match {
      case ConcreteTable(_, join) => join.length > 0
      case DerivedTable(_, s, join) => join.length > 0
    }

    stmt match {
      case s@Select(projection, _, _, None, _, _) if hasAggregate(projection) => One
      case s@Select(_, tableRefs, where, _, _, _) => 
        if ((tableRefs.length == 1 && !hasJoin(tableRefs.head) && 
             where.isDefined && hasNoOrExprs(s) && 
             typed.uniqueConstraints(tableRefs.head.tables.head).exists(c => inWhereClause(s, c))) || 
            hasLimit1(s))
          ZeroOrOne
        else 
          Many
      case Insert(_, _, SelectedInput(s)) => analyzeResults(s, typed)
      case Insert(_, _, _) => One
      case Update(_, _, _, _, _) => One
      case Delete(_, _) => One
      case Create() => One
      case SetStatement(s1, _, s2, _, _) => 
        analyzeResults(s1, typed) max analyzeResults(s2, typed)
      case Composed(s1, s2) => 
        analyzeResults(s1, typed) max analyzeResults(s2, typed)
    }
  }

  /**
   * Selected column is not optional if,
   * 
   * - it is restricted with IS NOT NULL and the expression contains only 'and' operators
   */
  private def analyzeSelection(stmt: Statement, typed: TypedStatement): TypedStatement = {
    def isNotNull(col: Named, where: Where) = (where.expr find {
      case Comparison1(t, IsNotNull) if t == col.term => 
        true
      case _ => 
        false
    }).isDefined

    stmt match {
      case s@Select(projection, _, Some(where), _, _, _) if hasNoOrExprs(s) => 
        typed.copy(output = projection.zip(typed.output) collect { case (n, t) =>
          if (t.nullable && isNotNull(n, where)) t.copy(nullable = false)
          else t
        })
      case _ => typed
    }
  }

  private def hasNoOrExprs(s: Select) = 
    s.where.map(w => !w.expr.find { 
      case Or(_, _) => true
      case _ => false 
    }.isDefined) getOrElse false
}

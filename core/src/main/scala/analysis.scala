package sqltyped

import Ast._
import NumOfResults._

class Analyzer(typer: Typer) extends Ast.Resolved {
  def refine(stmt: TypedStatement): ?[TypedStatement] = 
    stmt.copy(numOfResults = analyzeResults(stmt)).ok

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
  private def analyzeResults(stmt: TypedStatement): NumOfResults = {
    import scala.math.Ordering.Implicits._

    def hasNoOrExprs(s: Select) = 
      s.where.map(w => !w.expr.find { case Or(_, _) => true; case _ => false }.isDefined).getOrElse(false)

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
      projection collect { case Named(_, _, Function(n, _)) => n } exists typer.isAggregate

    stmt.stmt match {
      case s@Select(projection, _, _, None, _, _) if hasAggregate(projection) => One
      case s@Select(_, from, where, _, _, _) => 
        if ((from.length == 1 && from.head.join.length == 0 && 
             where.isDefined && hasNoOrExprs(s) && 
             stmt.uniqueConstraints(s.from.head.table).exists(c => inWhereClause(s, c))) || 
            hasLimit1(s))
          ZeroOrOne
        else 
          Many
      case Insert(_, _, SelectedInput(s)) => analyzeResults(stmt.copy(stmt = s))
      case Insert(_, _, _) => One
      case Update(_, _, _, _, _) => One
      case Delete(_, _) => One
      case Create() => One
      case Union(s1, s2, _, _) => 
        analyzeResults(stmt.copy(stmt = s1)) max analyzeResults(stmt.copy(stmt = s2))
      case Composed(s1, s2) => 
        analyzeResults(stmt.copy(stmt = s1)) max analyzeResults(stmt.copy(stmt = s2))
    }
  }
}

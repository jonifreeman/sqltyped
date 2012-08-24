package sqltyped

object Analyzer {
  import Ast._

  def refine(stmt: TypedStatement): TypedStatement = 
    if (returnsMultipleResults(stmt)) stmt else stmt.copy(multipleResults = false)

  private def returnsMultipleResults(stmt: TypedStatement) = stmt.stmt match {
    case _: Select => true
    case _ => false
  }
}

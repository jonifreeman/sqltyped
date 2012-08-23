package sqltyped

object Analyzer {
  def refine(stmt: TypedStatement): TypedStatement = 
    if (returnsMultipleResults(stmt)) stmt else stmt.copy(multipleResults = false)

  private def returnsMultipleResults(stmt: TypedStatement) = true
}

package sqltyped

import java.sql._
import org.scalatest._
import shapeless.test._

class FailureSuite extends Example {
  import Tables._
  import Columns._

  test("ORDER BY references unknown column") {
    illTyped("""
      sql("select name, age from person order by unknown_column")
    """)
  }

  test("INSERT references unknown column") {
    illTyped("""
      sql("insert into peson(id, name, age, salary) values (?, ?, ?, ?)")
    """)
  }
}

package sqltyped

import java.sql._
import org.scalatest._
import shapeless.test._

class FailureSuite extends Example {
  test("ORDER BY references unknown column") {
    illTyped("""
      sql("select name, age from person order by unknown_column").apply
    """)
  }

  test("INSERT references unknown table") {
    illTyped("""
      sql("insert into peson(id, name, age, salary) values (?, ?, ?, ?)").apply(1, "joe", 10, 0)
    """)
  }

  test("INSERT has unmatching number of listed columns and input") {
    illTyped("""
      sql("insert into person(id, name, age) values (?, ?)").apply(1, "joe")
    """)

    illTyped("""
      sql("insert into person(id, name, age) values (?, ?, 10, ?)").apply(1, "joe", 10)
    """)
  }
}

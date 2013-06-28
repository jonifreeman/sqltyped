package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

class PostgreSQLExamples extends PostgreSQLConfig {
  test("Simple query") {
    sql("select name from person").apply === List("joe", "moe")

    sql("select sum(age) from person").apply === Some(50)
  }
}

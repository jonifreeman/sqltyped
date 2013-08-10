package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

class PostgreSQLExamples extends PostgreSQLConfig {
  test("Simple query") {
    sql("select name from person").apply === List("joe", "moe")

    sql("select sum(age) from person").apply === Some(50)
  }

  test("any, some and all") {
    sql("select age from person where name = any(?)").apply(Seq("joe", "moe")) === List(36, 14)
    sql("select age from person where name = some(?)").apply(Seq("joe", "moe")) === List(36, 14)
    sql("select age from person where name = all(?)").apply(Seq("joe")) === List(36)

    sql("select name from person where age = any(?)").apply(Seq(1, 36)) === List("joe")
    sql("select name from person where age+1 = any(?)").apply(Seq(1, 37)) === List("joe")
  }
}

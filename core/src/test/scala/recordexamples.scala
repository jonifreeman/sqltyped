package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

class RecordExampleSuite extends MySQLConfig {
  test("Query to CSV") {
    val rows = sql("select name as fname, age, img from person limit 100").apply.values

    CSV.fromList(rows) === """ "joe","36",""
                              |"moe","14","" """.stripMargin.trim
  }

  test("Query to untyped tuples") {
    val rows = sql("select name, age from person limit 100").apply
    Record.toTupleList(rows.head) === List(("name", "joe"), ("age", 36))
    Record.toTupleLists(rows) === 
      List(List(("name", "joe"), ("age", 36)), List(("name", "moe"), ("age", 14)))
  }
}

package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

class MySQLExamples extends Example {
  import Tables._
  import Columns._

  test("Interval") {
    sql("select started + interval 1 month from job_history order by started").apply should
      equal(List(date("2002-09-02 08:00:00.0"), date("2004-08-13 11:00:00.0"), date("2005-09-10 11:00:00.0")))
  }
}

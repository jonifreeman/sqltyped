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

  test("Functions") {
//    val date = sql("select datediff(resigned, '2010-10-10') from job_history where resigned IS NOT NULL").apply.head
//    date.map(d => math.abs(d)) should equal(Some(2301))

//    val resignedQ = sql("select name from job_history where datediff(resigned, ?) < ?")
//    resignedQ.apply(date("2004-08-13 11:00:00.0"), 60) equal(List("Enron"))

/*
    sql("select coalesce(resigned, '1990-01-01 12:00:00') from job_history order by resigned").apply should
      equal(List(date("1990-01-01 12:00:00.0"), date("1990-01-01 12:00:00.0"), date("2004-06-22 18:00:00.0")))

//    sql("select coalesce(resigned, NULL) from job_history order by resigned").apply should
//      equal(List(None, None, Some(date("2004-06-22 18:00:00.0"))))

    sql("select ifnull(resigned, resigned) from job_history order by resigned").apply should
      equal(List(None, None, Some(date("2004-06-22 18:00:00.0"))))

    sql("select ifnull(resigned, started) from job_history order by resigned").apply should
      equal(List(date("2004-07-13 11:00:00.0"), date("2005-08-10 11:00:00.0"), date("2004-06-22 18:00:00.0")))

    sql("select coalesce(resigned, ?) from job_history order by resigned").apply(date("1990-01-01 12:00:00.0")) should
      equal(List(date("1990-01-01 12:00:00.0"), date("1990-01-01 12:00:00.0"), date("2004-06-22 18:00:00.0")))
*/
  }
}

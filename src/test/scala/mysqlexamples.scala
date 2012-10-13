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
    val d = sql("select datediff(resigned, '2010-10-10') from job_history where resigned IS NOT NULL").apply.head
    (d map math.abs) === Some(2301)

    // FIXME add support for input params as function args
    //val resignedQ = sql("select name from job_history where datediff(resigned, ?) < ?")
    //resignedQ.apply(date("2004-08-13 11:00:00.0"), 60) === List("Enron")

    sql("select coalesce(resigned, '1990-01-01 12:00:00') from job_history order by resigned").apply ===
      List(date("1990-01-01 12:00:00.0"), date("1990-01-01 12:00:00.0"), date("2004-06-22 18:00:00.0"))

    sql("select coalesce(resigned, NULL) from job_history order by resigned").apply ===
      List(None, None, Some(date("2004-06-22 18:00:00.0")))

    sql("select ifnull(resigned, resigned) from job_history order by resigned").apply ===
      List(None, None, Some(date("2004-06-22 18:00:00.0")))

    sql("select ifnull(resigned, started) from job_history order by resigned").apply ===
      List(date("2004-07-13 11:00:00.0"), date("2005-08-10 11:00:00.0"), date("2004-06-22 18:00:00.0"))

    sql("select coalesce(resigned, ?) from job_history order by resigned").apply(date("1990-01-01 12:00:00.0")) ===
      List(date("1990-01-01 12:00:00.0"), date("1990-01-01 12:00:00.0"), date("2004-06-22 18:00:00.0"))
  }

  test("Insert/update ignore") {
    val addPerson = sql("insert ignore into person(id, name, age, salary) values (?, ?, ?, ?)")
    val updateId  = sql("update ignore person set id=? where id=?")
    
    addPerson.apply(1, "tom", 40, 1000) === 0
  }
}

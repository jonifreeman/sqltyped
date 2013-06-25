package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

class MySQLExamples extends MySQLConfig {
  test("Interval") {
    sql("select started + interval 1 month from job_history order by started").apply should
      equal(List(tstamp("2002-09-02 08:00:00.0"), tstamp("2004-08-13 11:00:00.0"), tstamp("2005-09-10 11:00:00.0")))
  }

  test("Functions") {
    val d = sql("select datediff(resigned, '2010-10-10') from job_history where resigned IS NOT NULL").apply.head
    (d map math.abs) === Some(2301)

    val resignedQ = sql("select name from job_history where datediff(resigned, ?) < ?")
    resignedQ.apply(tstamp("2004-08-13 11:00:00.0"), Some(60)) === List("Enron")

    sql("select coalesce(resigned, '1990-01-01 12:00:00') from job_history order by resigned").apply ===
      List(tstamp("1990-01-01 12:00:00.0"), tstamp("1990-01-01 12:00:00.0"), tstamp("2004-06-22 18:00:00.0"))

    sql("select coalesce(resigned, NULL) from job_history order by resigned").apply ===
      List(None, None, Some(tstamp("2004-06-22 18:00:00.0")))

    sql("select ifnull(resigned, resigned) from job_history order by resigned").apply ===
      List(None, None, Some(tstamp("2004-06-22 18:00:00.0")))

    sql("select ifnull(resigned, started) from job_history order by resigned").apply ===
      List(tstamp("2004-07-13 11:00:00.0"), tstamp("2005-08-10 11:00:00.0"), tstamp("2004-06-22 18:00:00.0"))

    sql("select coalesce(resigned, ?) from job_history order by resigned").apply(tstamp("1990-01-01 12:00:00.0")) ===
      List(tstamp("1990-01-01 12:00:00.0"), tstamp("1990-01-01 12:00:00.0"), tstamp("2004-06-22 18:00:00.0"))

    sql("select IF(age<18 or age>100, 18, age) from person where age > ? order by age").apply(5) ===
      List(18, 36)
  }

  test("String functions") {
    sql("select concat('hello ', name, ?) from person").apply("!") === 
      List("hello joe!", "hello moe!")
  }

  test("Insert/update ignore") {
    val addPerson = sql("insert ignore into person(id, name, age, salary) values (?, ?, ?, ?)")
    val updateId  = sql("update ignore person set id=? where id=?")
    
    addPerson(1, "tom", 40, 1000) === 0
  }

  test("ON DUPLICATE KEY") {
    val addOrUpdate = sql(""" 
          insert into person(id, name, age, salary) values (?, ?, ?, ?)
          on duplicate key update name=?, age=age+1, salary=?
    """)

    addOrUpdate(1, "tom", 40, 1000, "tommy", 2000)
    sql("select name, age, salary from person where id=1").apply.tuples === 
      Some(("tommy", 37, 2000))
  }

  test("Types") {
    val q = sql("select * from alltypes LIMIT 1").apply.tuples
    q === Some((1, 1, 1, 1, 1, 1.0f, 1.0, 1.0, true, 
                date("2012-10-10"),
                time("14:00:00.0"),
                datetime("2012-10-10 00:00:00.0"),
                tstamp("2012-10-10 00:00:00.0"),
                date("2012-01-01"),
                "a", "a", "a", "v1", "v1", BigDecimal(1.0)))

    sql("update alltypes set t=? where a>100").apply(BigDecimal(1.0))
  }

  test("Cast functions") {
    sql("select binary(name) from person").apply === List("joe", "moe")

    sql("select binary(age) from person").apply === List(36, 14)

    val res1: List[String] = sql("select convert(age, char(10)) from person").apply
    res1 === List("36", "14")

    val res2: List[Int] = sql("select convert(name, signed) from person").apply
    res2 === List(0, 0)

    sql("select convert(name using utf8) from person").apply === List(Option("joe"), Option("moe"))
  }
}


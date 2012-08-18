package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

class ExampleSuite extends FunSuite with matchers.ShouldMatchers {
  Class.forName("com.mysql.jdbc.Driver")

  object Columns { object name; object age; object salary; object employer; object started
                   object resigned }

  implicit val c = Configuration(Columns)
  implicit def conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sqltyped", "root", "")

  import Columns._

  test("Simple query") {
    val q = sql("select name, age from person")
    q().map(_.get(age)).sum should equal (50)
  }

  test("Query with input") {
    val q = sql("select name, age from person where age > ? order by name")
    q(30).map(_.get(name)) should equal (List("joe"))
    q(10).map(_.get(name)) should equal (List("joe", "moe"))

    val q2 = sql("select name, age from person where age > ? and name != ? order by name")
    q2(10, "joe").map(_.get(name)) should equal (List("moe"))
  }

  test("Query with join and column alias") {
    val q = sql("select p.name, j.name as employer, p.age from person p join job_history j on p.id=j.person order by employer")

    q().values should equal (List("joe" :: "Enron" :: 36 :: HNil, "joe" :: "IBM" :: 36 :: HNil))
    q().tuples should equal (List(("joe", "Enron", 36), ("joe", "IBM", 36)))
  }

  test("Query with optional column") {
    val q = sql("select p.name, j.name as employer, j.started, j.resigned from person p join job_history j on p.id=j.person order by employer")
    
/*    q().tuples should equal (List(
      ("joe", "Enron", date("2002-08-02 12:00:00"), Some(date("2004-06-22 18:00:00"))), 
      ("joe", "IBM",   date("2004-07-13 11:00:00"), None)))*/
  }

/*  test("Query with just one selected column") {
      val q = sql("select name from person where age > ? order by name")
      q(10) should equal (List("joe", "moe"))    
  }*/

  def date(s: String) = 
    new java.sql.Timestamp(new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(s).getTime)
}

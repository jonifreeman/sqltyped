package sqltyped

import java.sql._
import org.scalatest._
import Sql._ // FIXME move to package object

class ExampleSuite extends FunSuite with matchers.ShouldMatchers {
  Class.forName("com.mysql.jdbc.Driver")

  object Columns { object name; object age; object salary; object employer }

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
    // FIXME cleanup
    q().values.map(_.tupled) should equal (List(("joe", "Enron", 36), ("joe", "IBM", 36)))
  }

/*  test("Query with just one selected column") {
      val q = sql("select name from person where age > ? order by name")
      q(10) should equal (List("joe", "moe"))    
  }*/
}

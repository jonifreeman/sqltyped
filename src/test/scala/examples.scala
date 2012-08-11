package sqltyped

import java.sql._
import org.scalatest._
import Sql._ // FIXME move to package object

class ExampleSuite extends FunSuite with matchers.ShouldMatchers {
  Class.forName("com.mysql.jdbc.Driver")

  object Columns { object name; object age; object salary }

  implicit val c = Configuration(Columns)
  val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sqltyped", "root", "")

  import Columns._

  test("Simple query") {
    val q = sql("select name, age from person")
    query(conn, q).map(p => p.get(age)).sum should equal (50)
  }
}

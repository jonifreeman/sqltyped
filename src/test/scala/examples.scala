package sqltyped

import org.scalatest._
import Sql._ // FIXME move to package object

class ExampleSuite extends FunSuite with matchers.ShouldMatchers {
  object Columns {
    object name
    object age
  }

  implicit val c = Configuration("jdbc:mysql://localhost:3306/sqltyped", "com.mysql.jdbc.Driver", "root", "", Columns)

  import Columns._

  test("Simple query") {
    val q = sql("select name, age from person")
    query(q).map(p => p.get(age)).sum should equal (50)
  }
}

package sqltyped

import org.scalatest._
import Sql._ // FIXME move to package object

class ExampleSuite extends FunSuite {
  object Columns {
    object name
    object age
  }

  test("Simple query") {
    implicit val c = Configuration("jdbc:mysql://localhost:3306/sqltyped", "com.mysql.jdbc.Driver", "root", "", Columns)

    val q = sql("select name, age from person")
    query(q).map(_.get(age)).sum must equal (50)
  }
}

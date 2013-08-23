package sqltyped.slick

import scala.slick.session.Database
import org.scalatest._
import sqltyped._

class SlickExample extends FunSuite with BeforeAndAfterEach with matchers.ShouldMatchers {
  val db = Database.forURL("jdbc:mysql://localhost:3306/sqltyped", 
                           driver = "com.mysql.jdbc.Driver", user = "root", password = "")

  implicit val c = Configuration()
  implicit def conn = Database.threadLocalSession.conn

  override def beforeEach() {
    val newPerson  = sql("insert into person(id, name, age, salary) values (?, ?, ?, ?)")

    db withSession {
      sql("delete from person").apply

      newPerson(1, "joe", 36, 9500)
      newPerson(2, "moe", 14, 8000)
    }
  }

  test("with session") {
    val q = sql("select name, age from person where age > ?")

    db withSession {
      q(30).tuples should equal(List(("joe", 36)))
    }
  }

  test("transaction") {
    db withTransaction {
      sql("update person set name=? where id=?").apply("danny", 1)
      Database.threadLocalSession.rollback
    }

    db withSession {
      sql("select name from person where id=?").apply(1) should equal(Some("joe"))
    }
  }
}


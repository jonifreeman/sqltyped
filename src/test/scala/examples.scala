package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

class ExampleSuite extends FunSuite with BeforeAndAfterEach with matchers.ShouldMatchers {
  Class.forName("com.mysql.jdbc.Driver")

  object Tables { trait person; trait job_history }
  object Columns { object name; object age; object salary; object employer; object started
                   object resigned; object avg; object count }

  implicit val c = Configuration(Tables, Columns)
  implicit def conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sqltyped", "root", "")

  import Tables._
  import Columns._

  override def beforeEach() {
/*
    val newPerson  = sql("insert into person(id, name, age, salary) values (?, ?, ?, ?)").apply _
    val jobHistory = sql("insert into job_history values (?, ?, ?, ?").apply _

    sql("delete from job_history")
    sql("delete from person")

    newPerson(1, "joe", 36, 9500)
    newPerson(2, "moe", 14, 8000)
    jobHistory(1, "Enron", date("2002-08-02 08:00:00"), Some(date("2004-06-22 18:00:00")))
    jobHistory(1, "IBM", date("2004-07-13 11:00:00"), None)
    jobHistory(2, "IBM", date("2005-08-10 11:00:00"), None)
*/
  }

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
    val q = sql("select p.name, j.name as employer, p.age from person p join job_history j on p.id=j.person where id=? order by employer")

    q(1).values should equal (List("joe" :: "Enron" :: 36 :: HNil, "joe" :: "IBM" :: 36 :: HNil))
    q(1).tuples should equal (List(("joe", "Enron", 36), ("joe", "IBM", 36)))
  }

  test("Query with optional column") {
    val q = sql("select p.name, j.name as employer, j.started, j.resigned from person p join job_history j on p.id=j.person order by employer")
    
    q().tuples should equal (List(
      ("joe", "Enron", date("2002-08-02 08:00:00.0"), Some(date("2004-06-22 18:00:00.0"))), 
      ("joe", "IBM",   date("2004-07-13 11:00:00.0"), None),
      ("moe", "IBM",   date("2005-08-10 11:00:00.0"), None)))
  }

  test("Query with functions") {
    val q = sql("select avg(age), sum(salary) as salary, count(1) from person where abs(age) > ?")
    val res = q(10).head // FIXME .head is redundant, this query always returns just one row
    res.get(avg) should equal(Some(25.0))
    res.get(salary) should equal(Some(17500))
    res.get(count) should equal(2)

    val q2 = sql("select min(name) as name, max(age) as age from person where age > ?")
    val res2 = q2(10).head
    res2.get(name) should equal(Some("joe"))
    res2.get(age) should equal(Some(36))
    
    val res3 = q2(100).head
    res3.get(name) should equal(None)
    res3.get(age) should equal(None)
  }

  test("Query with just one selected column") {
    val q = sql("select name from person where age > ? order by name")
    q(10) should equal (List("joe", "moe"))    
  }
  
  test("Query with constraint by unique column") {
    val q = sql("select age, name from person where id=?")
    q(1) should equal (Some((age -> 36) :: (name -> "joe") :: HNil))
    q(1).tuples should equal (Some(36, "joe"))
    
    val q2 = sql("select name from person where id=?")
    q2(1) should equal (Some("joe"))
    
    val q3 = sql("select name from person where id=? and age>?")
    q3(1, 10) should equal (Some("joe"))
    
    val q4 = sql("select name from person where id=? or age>?")
    q4(1, 10) should equal (List("joe", "moe"))

    val q5 = sql("select age from person order by age desc limit 1")
    q5() should equal (Some(36))
  }

  test("Query with limit") {
    sql("select age from person order by age limit ?").apply(2) should equal(List(14, 36))
    sql("select age from person order by age limit ?").apply(1) should equal(List(14))
    sql("select age from person order by age limit ? offset 1").apply(1) should equal(List(36))
    sql("select age from person order by age limit ? offset ?").apply(1, 1) should equal(List(36))

    val q = sql("select age from person where name between ? and ? limit ?")
    q("i", "k", 2) should equal(List(36))
  }

  test("Tagging") {
    def findName(id: Long @@ person) = sql("select name from person where id=?").apply(id)

    val names = sql("select distinct person from job_history").apply map findName
    names should equal(List(Some("joe"), Some("moe")))
  }

  test("Subselects") {
    sql("select distinct name from person where id = (select person from job_history limit 1)").apply should
      equal(Some("joe"))

    sql("select distinct name from person where id in (select person from job_history)").apply should
      equal(List("joe", "moe"))

    sql("""select distinct name from person where id in 
             (select person from job_history where started > ?)""").apply(year(2003)) should
      equal(List("joe", "moe")) 

    sql("""select name from person p where exists 
             (select person from job_history j where resigned is not null and p.id=j.person)""").apply should
      equal(List("joe"))

    sql("""select name from person p where not exists 
             (select person from job_history j where resigned is not null and p.id=j.person)""").apply should
      equal(List("moe"))
  }

  def date(s: String) = 
    new java.sql.Timestamp(new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S").parse(s).getTime)

  def year(y: Int) = date(y + "-01-01 00:00:00.0")
}

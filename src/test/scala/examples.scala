package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

trait Example extends FunSuite with BeforeAndAfterEach with matchers.ShouldMatchers {
  Class.forName("com.mysql.jdbc.Driver")
//  Class.forName("org.postgresql.Driver")

  object Tables { trait person; trait job_history }
  object Columns { object name; object age; object salary; object count; object avg }

  implicit val c = Configuration(Tables, Columns)
  implicit val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sqltyped", "root", "")
//  implicit val conn = DriverManager.getConnection("jdbc:postgresql://localhost/sqltyped", "sqltypedtest", "secret")

  override def beforeEach() {
    val newPerson  = sql("insert into person(id, name, age, salary) values (?, ?, ?, ?)")
    val jobHistory = sql("insert into job_history values (?, ?, ?, ?)")

    sql("delete from job_history").apply
    sql("delete from person").apply

    newPerson(1, "joe", 36, 9500)
    newPerson(2, "moe", 14, 8000)

    jobHistory(1, "Enron", date("2002-08-02 08:00:00.0"), Some(date("2004-06-22 18:00:00.0")))
    jobHistory(1, "IBM", date("2004-07-13 11:00:00.0"), None)
    jobHistory(2, "IBM", date("2005-08-10 11:00:00.0"), None)
  }

  def date(s: String) = 
    new java.sql.Timestamp(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(s).getTime)

  def year(y: Int) = date(y + "-01-01 00:00:00.0")

  implicit class TypeSafeEquals[A](a: A) {
    def ===(other: A) = a should equal(other)
  }
}

class ExampleSuite extends Example {
  import Tables._
  import Columns._

  test("Simple query") {
    val q1 = sql("select name, age from person")
    q1().map(_.get(age)).sum === 50

    val q2 = sql("select * from person")
    q2().map(_.get(age)).sum === 50
  }

  test("Query with input") {
    val q = sql("select name, age from person where age > ? order by name")
    q(30).map(_.get(name)) === List("joe")
    q(10).map(_.get(name)) === List("joe", "moe")

    val q2 = sql("select name, age from person where age > ? and name != ? order by name")
    q2(10, "joe").map(_.get(name)) === List("moe")
  }

  test("Query with join and column alias") {
    val q = sql("select p.name, j.name as employer, p.age from person p join job_history j on p.id=j.person where id=? order by employer")

    q(1).values === List("joe" :: "Enron" :: 36 :: HNil, "joe" :: "IBM" :: 36 :: HNil)
    q(1).tuples === List(("joe", "Enron", 36), ("joe", "IBM", 36))
  }

  test("Query with optional column") {
    val q = sql("select p.name, j.name as employer, j.started, j.resigned from person p join job_history j on p.id=j.person order by j.started")
    
    q().tuples === List(
      ("joe", "Enron", date("2002-08-02 08:00:00.0"), Some(date("2004-06-22 18:00:00.0"))), 
      ("joe", "IBM",   date("2004-07-13 11:00:00.0"), None),
      ("moe", "IBM",   date("2005-08-10 11:00:00.0"), None))
  }

  test("Query with functions") {
    val q = sql("select avg(age), sum(salary) as salary, count(1) from person where abs(age) > ?")
    val res = q(10)
    res.get(avg) === Some(25.0)
    res.get(salary) === Some(17500)
    res.get(count) === 2

    val q2 = sql("select min(name) as name, max(age) as age from person where age > ?")
    val res2 = q2(10)
    res2.get(name) === Some("joe")
    res2.get(age) === Some(36)
    
    val res3 = q2(100)
    res3.get(name) === None
    res3.get(age) === None

    sql("select min(?) from person").apply(10: java.lang.Long) should equal(Some(10))

    sql("select max(age) from person").apply === Some(36)

    sql("select count(id) from person").apply === 2

    sql("select max(id) from person where age > ?").apply(100) === None
  }

  test("Query with just one selected column") {
    sql("select name from person where age > ? order by name").apply(10) === 
      List("joe", "moe")

    sql("select name from person where age > ? order by name for update").apply(10) === 
      List("joe", "moe")
  }
  
  test("Query with constraint by unique column") {
    val q = sql("select age, name from person where id=?")
    q(1) === Some((age -> 36) :: (name -> "joe") :: HNil)
    q(1).tuples === Some((36, "joe"))
    
    val q2 = sql("select name from person where id=?")
    q2(1) === Some("joe")
    
    val q3 = sql("select name from person where id=? and age>?")
    q3(1, 10) === Some("joe")
    
    val q4 = sql("select name from person where id=? or age>?")
    q4(1, 10) === List("joe", "moe")

    val q5 = sql("select age from person order by age desc limit 1")
    q5() === Some(36)
  }

  test("Query with limit") {
    sql("select age from person order by age limit ?").apply(2) === List(14, 36)
    sql("select age from person order by age limit ?").apply(1) === List(14)
    sql("select age from person order by age limit ? offset 1").apply(1) === List(36)
    sql("select age from person order by age limit ? offset ?").apply(1, 1) === List(36)

    val q = sql("select age from person where name between ? and ? limit ?")
    q("i", "k", 2) === List(36)
  }

  test("Tagging") {
    def findName(id: Long @@ person) = sql("select name from person where id=?").apply(id)

    val names = sql("select distinct person from job_history").apply map findName
    names === List(Some("joe"), Some("moe"))

    sqlt("select name,age from person where id=?").apply(tag[person](1)).tuples === Some("joe", 36)
  }

  test("Subselects") {
    sql("select distinct name from person where id = (select person from job_history limit 1)").apply ===
      Some("joe")

    sql("select distinct name from person where id in (select person from job_history)").apply ===
      List("joe", "moe")

    sql("""select distinct name from person where id in 
             (select person from job_history where started > ?)""").apply(year(2003)) ===
      List("joe", "moe")

    sql("""select name from person p where exists 
             (select person from job_history j where resigned is not null and p.id=j.person)""").apply ===
      List("joe")

    sql("""select name from person p where not exists 
             (select person from job_history j where resigned is not null and p.id=j.person)""").apply ===
      List("moe")
  }

  test("Insert, delete") {
    sql("delete from jobs").apply
    sql("insert into jobs(person, job) select p.name, j.name from person p, job_history j where p.id=j.person and p.age>?").apply(30)
    sql("select person, job from jobs").apply.tuples ===
      List(("joe", "Enron"), ("joe", "IBM"))

    sql("delete from jobs where job=?").apply("Enron")
    sql("select person, job from jobs").apply.tuples === List(("joe", "IBM"))

    sql("delete p from person p where p.age < ?").apply(40)
    sql("select name from person").apply === Nil
  }

  test("Multidelete") {
    sql("delete p, j from person p, job_history j where p.id=j.person and p.id=?").apply(1)
    sql("select name from person").apply === List("moe")
  }

  test("Get generated keys (currently supports just one generated key per row)") {
    val newPerson  = sqlk("insert into person(name, age, salary) values (?, ?, ?)")
    val newPersons = sqlk("insert into person(name, age, salary) select name, age, salary from person")

    val key = newPerson.apply("bill", 45, 3000)
    val maxId = sql("select id from person where name=?").apply("bill").head 
    assert(key.equals(maxId))
    sqlt("delete from person where id=?").apply(key)

    newPersons.apply should equal(List(maxId + 1, maxId + 2))
  }

  test("Update") {
    sql("update person set name=? where age >= ?").apply("joe2", 30)
    sql("select name from person order by age").apply === List("moe", "joe2")

    sql("update person set name=? where age >= ? order by age asc limit 1").apply("moe2", 10)
    sql("select name from person order by age").apply === List("moe2", "joe2")

    sql("update person set name=upper(name)").apply
    sql("select name from person order by age").apply === List("MOE2", "JOE2")

    sql("update person p, job_history j set p.name=?, j.name=? where p.id=j.person and p.age > ?").apply("joe2", "x", 30)
    sql("select p.name, j.name from person p, job_history j where p.id=j.person order by age").apply.tuples ===
      List(("MOE2", "IBM"), ("joe2", "x"), ("joe2", "x"))
  }

  test("Blob") {
    import javax.sql.rowset.serial.SerialBlob

    val img = new SerialBlob("fake img".getBytes("UTF-8"))
    sql("update person set img=? where name=?").apply(Some(img), "joe")
    val savedImg = sql("select img from person where name=?").apply("joe").head.get.getBytes(1, 8)
    new String(savedImg, "UTF-8") === "fake img"
  }

  test("Union") {
    val q1 = sql(""" 
                 select name,age from person where age < ?
                 union 
                 select name,age from person where age > ?
                 """)
    q1.apply(15, 20).tuples === List(("moe", 14), ("joe", 36))

    val q2 = sql(""" 
                 (select name from person where age < ?)
                 union 
                 (select name from person where age > ?)
                 order by name desc limit ?
                 """)
    q2.apply(15, 20, 5) === List("moe", "joe")
  }

  test("Arithmetic") {
    sql("update person set age = age + 1").apply
    sql("select age - 1, age, age * 2, (age % 10) - 1 as age from person order by age").apply.tuples ===
      List((14, 15, 30, 4), (36, 37, 74, 6))
  }
}

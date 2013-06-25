package sqltyped

import java.sql._
import org.scalatest._
import shapeless._, TypeOperators._

trait Example extends FunSuite with BeforeAndAfterEach with matchers.ShouldMatchers {
  object Tables { trait person; trait job_history }
  object Columns { object name; object age; object salary; object count; object avg 
                   object select; object `type` }

  def beforeEachWithConfig[A, B](implicit config: Configuration[A, B], conn: Connection) {
    val newPerson  = sql("insert into person(id, name, age, salary) values (?, ?, ?, ?)")
    val jobHistory = sql("insert into job_history values (?, ?, ?, ?)")

    sql("delete from job_history").apply
    sql("delete from person").apply

    newPerson(1, "joe", 36, 9500)
    newPerson(2, "moe", 14, 8000)

    jobHistory(1, "Enron", tstamp("2002-08-02 08:00:00.0"), Some(tstamp("2004-06-22 18:00:00.0")))
    jobHistory(1, "IBM", tstamp("2004-07-13 11:00:00.0"), None)
    jobHistory(2, "IBM", tstamp("2005-08-10 11:00:00.0"), None)
  }

  def tstamp(s: String) = 
    new java.sql.Timestamp(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(s).getTime)

  def datetime(s: String) = 
    new java.sql.Timestamp(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(s).getTime)

  def date(s: String) = 
    new java.sql.Date(new java.text.SimpleDateFormat("yyyy-MM-dd").parse(s).getTime)

  def time(s: String) = 
    new java.sql.Time(new java.text.SimpleDateFormat("HH:mm:ss.S").parse(s).getTime)

  def year(y: Int) = tstamp(y + "-01-01 00:00:00.0")

  implicit class TypeSafeEquals[A](a: A) {
    def ===(other: A) = a should equal(other)
  }
}

trait PostgreSQLConfig extends Example {
  Class.forName("org.postgresql.Driver")

  implicit val config = Configuration(Tables, Columns)
  implicit object postgresql extends ConfigurationName
  implicit val conn = DriverManager.getConnection("jdbc:postgresql://localhost/sqltyped", "sqltypedtest", "secret")

  override def beforeEach() = beforeEachWithConfig
}

trait MySQLConfig extends Example {
  Class.forName("com.mysql.jdbc.Driver")

  implicit val config = Configuration(Tables, Columns)
  implicit val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sqltyped", "root", "")

  override def beforeEach() = beforeEachWithConfig
}

class ExampleSuite extends MySQLConfig {
  import Tables._
  import Columns._

  test("Simple query") {
    val q1 = sql("select name, age from person")
    q1().map(_.get(age)).sum === 50

    val q2 = sql("select * from person")
    q2().map(_.get(age)).sum === 50

    sql("select p.* from person p").apply.map(_.get(age)).sum === 50

    sql("select (name) n, (age) as a from person").apply.tuples ===
      List(("joe", 36), ("moe", 14))

    sql("select 'success' as status from person").apply ===
      List("success", "success")
  }

  test("Query with input") {
    val q = sql("select name, age from person where age > ? order by name")
    q(30).map(_.get(name)) === List("joe")
    q(10).map(_.get(name)) === List("joe", "moe")

    val q2 = sql("select name, age from person where age > ? and name != ? order by name")
    q2(10, "joe").map(_.get(name)) === List("moe")

    sql("select name, ? from person").apply("x").tuples ===
      List(("joe", "x"), ("moe", "x"))
  }

  test("Joins") {
    sql("SELECT distinct p.name FROM person p JOIN job_history j ON p.id=j.person").apply ===
      List("joe", "moe")

    sql("SELECT distinct p.name FROM (person p) INNER JOIN job_history j").apply ===
      List("joe", "moe")

    sql("SELECT distinct p.name FROM person p CROSS JOIN job_history j").apply ===
      List("joe", "moe")

    sql("SELECT p.name FROM person p JOIN (job_history j) ON (p.id=j.person and j.name=?)").apply("IBM") ===
      List("joe", "moe")

    sql("select p.name from person p join person p2 using (id)").apply === List("joe", "moe")

    val qNullable = sql("""
    select j.resigned from person p left join job_history j on p.id=j.person 
    where p.name=? and j.name=? LIMIT 1
    """)

    qNullable.apply("unknown", Some("IBM")) === None
    qNullable.apply("joe", Some("IBM"))     === Some(None)
    qNullable.apply("joe", Some("Enron"))   === Some(Some(tstamp("2004-06-22 18:00:00.0")))

    val qNonNullable = sql("""
    select j.started from person p left join job_history j on p.id=j.person 
    where p.name=? and j.name=? LIMIT 1
    """)

    qNonNullable.apply("unknown", Some("IBM")) === None
    qNonNullable.apply("joe", Some("unknown")) === None
    qNonNullable.apply("joe", Some("Enron"))   === Some(Some(tstamp("2002-08-02 08:00:00.0")))

    sql("""
        SELECT p.name 
        FROM person p 
        JOIN (
          SELECT * FROM person WHERE age>?
        ) AS p2 ON p.id=p2.id
        """).apply(10) === List("joe", "moe")
  }

  test("Query with join and column alias") {
    val q = sql("select p.name, j.name as employer, p.age from person p join job_history j on p.id=j.person where id=? order by employer")

    q(1).values === List("joe" :: "Enron" :: 36 :: HNil, "joe" :: "IBM" :: 36 :: HNil)
    q(1).tuples === List(("joe", "Enron", 36), ("joe", "IBM", 36))
  }

  test("Query with optional column") {
    val q = sql("select p.name, j.name as employer, j.started, j.resigned from person p join job_history j on p.id=j.person order by j.started")
    
    q().tuples === List(
      ("joe", "Enron", tstamp("2002-08-02 08:00:00.0"), Some(tstamp("2004-06-22 18:00:00.0"))), 
      ("joe", "IBM",   tstamp("2004-07-13 11:00:00.0"), None),
      ("moe", "IBM",   tstamp("2005-08-10 11:00:00.0"), None))
  }

  test("Group by and order by") {
    sql("select p.name from person p where age > ? group by p.id, p.age order by p.name").apply(1) ===
      List("joe", "moe")

    sql("select p.name from person p where age > ? order by abs(salary - age)").apply(1) ===
      List("moe", "joe")

    sql("select p.name from person p where age > ? order by ? desc").apply(5, 1) ===
      List("moe", "joe")

    sql("select p.name from person p where age > ? order by abs(salary - ?)").apply(1, 500) ===
      List("moe", "joe")

    sql("select p.name, p.age from person p where age > ? order by 2 desc").apply(1).tuples ===
      List(("joe", 36), ("moe", 14))

    sql("select p.name, p.age from person p where age > ? order by (2) desc, (p.name)").apply(1).tuples ===
      List(("joe", 36), ("moe", 14))

    sql("select p.name from person p where age > ? group by p.name collate latin1_swedish_ci order by p.name").apply(10) ===
      List("joe", "moe")

    sql("select p.name from person p where age > ? group by p.name collate latin1_swedish_ci order by p.name collate latin1_swedish_ci asc limit 2").apply(10) ===
      List("joe", "moe")

    sql("select p.name from person p where age > ? group by ucase(p.name)").apply(10) ===
      List("joe", "moe")
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

    sql("select min(?) from person").apply(10) should equal(Some(10))

    sql("select max(age) from person").apply === Some(36)

    sql("select max(age) + 1 from person").apply === Some(37)

    sql("select count(id) from person").apply === 2

    sql("select max(id) from person where age > ?").apply(100) === None

    sql("select age > 20 from person order by age").apply ===
      List(false, true)

    sql("select resigned is not null from job_history order by started").apply ===
      List(true, false, false)

    sql("select age in (1,36) from person order by age desc").apply ===
      List(true, false)

    sql("select resigned < now() from job_history order by started").apply ===
      List(Some(true), None, None)

    sql("select age from person where age|10=46").apply ===
      List(36)

    sql("select age from person where age|?=?").apply(10, 46) ===
      List(36)

    sql("select age|? from person where age&?=0").apply(10, 2) ===
      List(46)

    sql("select ? > 2 from person").apply(3) === 
      List(true, true)

    sql("select age > ? from person order by age").apply(18) ===
      List(false, true)

    sql("select count(age>?) as a2 from person").apply(30) === 2L

    sql("select age/(age*10) from person").apply === List(Some(0.1), Some(0.1))
    sql("select age/0 from person").apply === List(None, None)

    sql("select count(distinct name) from person").apply === 2
  }

  test("Query with just one selected column") {
    sql("select name from person where age > ? order by name").apply(10) === 
      List("joe", "moe")

    sql("select name from person where age > ? order by name for update").apply(10) === 
      List("joe", "moe")

    sql("select name from person where name LIKE ? order by name").apply("j%") === 
      List("joe")

    sql("select name from person where not(age > ? and name=?)").apply(10, "joe") === 
      List("moe")
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

    sql("select count(1) from person where id=?").apply(1) === 1
    sql("select count(1) from person where id=?").apply(999) === 0
    sql("select count(1) > 0 from person where id=?").apply(1) === true
  }

  test("Query with is not null") {
    val q1 = 
      sql("""
          SELECT j.resigned FROM person p LEFT JOIN job_history j ON p.id=j.person 
          WHERE p.name=? AND j.resigned IS NOT NULL
          """)
    q1.apply("joe") === List(tstamp("2004-06-22 18:00:00.0"))

    val q2 = 
      sql("""
          SELECT j.resigned FROM person p LEFT JOIN job_history j ON p.id=j.person 
          WHERE (p.name=? AND j.resigned IS NOT NULL) OR p.age>?
          """)
    q2.apply("joe", 100) === List(Some(tstamp("2004-06-22 18:00:00.0")))
  }

  test("Query with limit") {
    sql("select age from person order by age limit ?").apply(2) === List(14, 36)
    sql("select age from person order by age desc, name asc limit ?").apply(2) === List(36, 14)
    sql("select age from person order by age limit ?").apply(1) === List(14)
    sql("select age from person order by age limit ? offset 1").apply(1) === List(36)
    sql("select age from person order by age limit ? offset ?").apply(1, 1) === List(36)

    val q = sql("select age from person where name between ? and ? limit ?")
    q("i", "k", 2) === List(36)

    sql("select age from person where name not between ? and ?").apply("l", "z") ===
      List(36)
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

    sql("select distinct name from person where id not in (select person from job_history)").apply ===
      Nil

    sql("""select distinct name from person where id in 
             (select person from job_history where started > ?)""").apply(year(2003)) ===
      List("joe", "moe")

    sql("""select name from person p where exists 
             (select person from job_history j where resigned is not null and p.id=j.person)""").apply ===
      List("joe")

    sql("""select name from person p where not exists 
             (select person from job_history j where resigned is not null and p.id=j.person)""").apply ===
      List("moe")

    sql(""" 
        select p.name from person p where p.age <= ? and ? <
          (select j.started from job_history j where p.id=j.person limit 1) order by p.name
        """).apply(50, tstamp("2002-08-02 08:00:00.0")) ===
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

    sql("""
        insert into person(salary, name, age) 
        values ((select count(1) from job_history where name = ? LIMIT 1), ?, ?)
        """).apply("IBM", "foo", 50)
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

    sql("UPDATE person SET age=age&? WHERE name=?").apply(0, "joe2")
    sql("SELECT age FROM person WHERE name=?").apply("joe2").head === 0
    
    sql("update alltypes set i=not(i) where a > 100").apply
  }

  test("Blob") {
    import javax.sql.rowset.serial.SerialBlob

    val img = new SerialBlob("fake img".getBytes("UTF-8"))
    sql("update person set img=? where name=?").apply(Some(img), "joe")
    val savedImg = sql("select img from person where name=?").apply("joe").head.get.getBytes(1, 8)
    new String(savedImg, "UTF-8") === "fake img"
  }

  test("Union") {
    sql(""" 
        select name,age from person where age < ?
        union 
        select name,age from person where age > ?
        """).apply(15, 20).tuples === List(("moe", 14), ("joe", 36))

    sql(""" 
        (select name from person where age < ?)
        union all
        (select name from person where age > ?)
        order by name desc limit ?
        """).apply(15, 20, 5) === List("moe", "joe")
  }

  test("Arithmetic") {
    sql("select SUM(age + age) from person where age<>?").apply(40) ===
      Some(100)

    sql("update person set age = age + 1").apply
    sql("select age - 1, age, age * 2, (age % 10) - 1 as age, -10 from person order by age").apply.tuples ===
      List((14, 15, 30, 4, -10), (36, 37, 74, 6, -10))
  }

  test("Quoting") {
    val rows = sql(""" select `in`.name as "select", age > 20 as "type" from person `in` order by age """).apply
    
    rows.head.get(select) === "moe"
    rows.head.get(`type`) === false
  }

  test("DUAL table") {
    sql("select count(1) from dual").apply === 1
  }

  test("Subselect as a relation") {
    sql("select id from (select id, name from person) AS data where data.name = ?").apply("joe") ===
      List(1)

    sql("select id from (select id, name from person where age>?) data where data.name = ?").apply(20, "joe") ===
      List(1)

    sql("""
        SELECT data.id, j.name 
        FROM (select id, name from person) AS data join job_history j on (data.id = j.person) 
        WHERE data.name = ?
        """).apply("joe").tuples === List((1, "Enron"), (1, "IBM"))

    sql("""
        SELECT data.id, j.name 
        FROM 
          (select id, name from person where age>20) AS data join 
          (select person,name from job_history) AS j on (data.id=j.person) 
        WHERE data.name = ?
        """).apply("joe").tuples === List((1, "Enron"), (1, "IBM"))
  }

  test("Subselect in projection") {
    sql("""
        select p.name, (select name from job_history where p.age > ? and person=p.id limit 1) 
        from person p order by p.name
        """).apply(20).tuples === List(("joe", Some("Enron")), ("moe", None))
  }

  test("Fallback by using an obscure unsupported MySQL syntax (order by NULL)") {
    sql("select name, age from person where age > ? order by NULL").apply(5).tuples ===
      List(("joe", 36), ("moe", 14))

    sql("select name, age from person where age > ? order by NULL LIMIT 1").apply(5).tuples ===
      List(("joe", 36))
  }
}

package sqltyped

import org.scalatest._
import sqltyped.SqlParser._
import sqltyped.Ast._
import scala.reflect.runtime.universe.typeOf

class ParserSuite extends FunSuite with matchers.ShouldMatchers {
  test("Simple queries") {
    testParse("select name,age from person", 
              "select name, age from person")

    testParse("select name,age from person where age > 10",
              "select name, age from person where age > 10")

    testParse("select name,age from person where age = 10 order by age",
              "select name, age from person where age = 10 order by age")

    testParse("select name,age from person where age < 10 or age > 20 order by age asc",
              "select name, age from person where (age < 10 or age > 20) order by age asc")
  }

  test("Alias queries") {
    testParse("select p.name,p.age from person as p",
              "select p.name, p.age from person as p")

    testParse("select p.name, p.age from person as p where p.age > 10",
              "select p.name, p.age from person as p where p.age > 10")

    testParse("select p.name,p.age from person p where p.age < 10 order by p.age",
              "select p.name, p.age from person as p where p.age < 10 order by p.age")

    testParse("select p.name,p.age from person as p where p.age = 10 and p.name='joe' order by p.age asc",
              "select p.name, p.age from person as p where (p.age = 10 and p.name = 'joe') order by p.age asc")

    testParse("select name as personname from person",
              "select name as personname from person")

    testParse("select p.name as personname from person as p",
              "select p.name as personname from person as p")
  }

  test("Variable queries") {
    testParse("select name, person.age from person where age > ?",
              "select name, person.age from person where age > ?")

    testParse("select name, person.age from person where person.age > ?",
              "select name, person.age from person where person.age > ?")

    testParse("select p.name, p.age from person as p where p.age > ?",
              "select p.name, p.age from person as p where p.age > ?")

    testParse("select p.name,p.age from person as p where p.age = ? and p.name=? order by p.age asc",
              "select p.name, p.age from person as p where (p.age = ? and p.name = ?) order by p.age asc")
  }

  test("Equi joins") {
    testParse("select p.name, c.age from person as p, child c where p.name > c.parent",
              "select p.name, c.age from person as p, child as c where p.name > c.parent")

    testParse("select p.name, c.age from person as p, child c where p.name > c.parent and c.id>?",
              "select p.name, c.age from person as p, child as c where (p.name > c.parent and c.id > ?)")
  }

  test("Joins") {
    testParse("select p.name, j.name as employer, j.started, j.resigned from person p join job_history j on p.id=j.person",
              "select p.name, j.name as employer, j.started, j.resigned from person as p join job_history as j on p.id = j.person")

    testParse("select j.name from person p inner join job_history j on p.id=j.person",
              "select j.name from person as p inner join job_history as j on p.id = j.person")

    testParse("select j.name from person p left outer join job_history j on p.id=j.person",
              "select j.name from person as p left outer join job_history as j on p.id = j.person")

    testParse("select j.name from person p right outer join job_history j on p.id=j.person",
              "select j.name from person as p right outer join job_history as j on p.id = j.person")

    testParse("select j.name from person p join job_history j on p.id=j.person where j.age>?",
              "select j.name from person as p join job_history as j on p.id = j.person where j.age > ?")
  }

  test("Functions") {
    testParse(
      "select name, AVG(price), SUM(price) as p from titles group by title having AVG(price) > ? and COUNT(price) > 100",
      "select name, AVG(price), SUM(price) as p from titles group by title having (AVG(price) > ? and COUNT(price) > 100)")
  }

  def testParse(sql: String, formattedAst: String) = {
    val Right(ast) = parse(sql)
    ast.toSql should equal(formattedAst)
    parse(formattedAst) should equal(Right(ast))
  }
}

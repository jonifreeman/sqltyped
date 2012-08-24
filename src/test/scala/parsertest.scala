package sqltyped

import org.scalatest._
import sqltyped.SqlParser._
import sqltyped.Ast._
import scala.reflect.runtime.universe.typeOf

class ParserSuite extends FunSuite with matchers.ShouldMatchers {
  test("Simple queries") {
    parse("select name,age from person") should 
      equal(Right(
        Select(List(Column("name", None, None), Column("age", None, None)),
               List(From(Table("person", None), Nil)), None, None, None, None)))

    parse("select name,age from person where age > 10") should 
      equal(Right(
        Select(List(Column("name", None, None), Column("age", None, None)),
               List(From(Table("person", None), Nil)), 
               Some(Where(Predicate(Column("age", None, None), Gt, Constant(typeOf[Long])))), 
               None, None, None)))

    parse("select name,age from person where age = 10 order by age") should 
      equal(Right(
        Select(List(Column("name", None, None), Column("age", None, None)),
               List(From(Table("person", None), Nil)),
               Some(Where(Predicate(Column("age", None, None), Eq, Constant(typeOf[Long])))),
               None, Some(OrderBy(List(Column("age", None, None)), None)), None)))

    parse("select name,age from person where age < 10 or age > 20 order by age asc") should 
      equal(Right(
        Select(List(Column("name", None, None), Column("age", None, None)),
               List(From(Table("person", None), Nil)),
               Some(Where(Or(Predicate(Column("age", None, None), Lt, Constant(typeOf[Long])),
                             Predicate(Column("age", None, None), Gt, Constant(typeOf[Long]))))),
               None, Some(OrderBy(List(Column("age", None, None)), Some(Asc))), None)))
  }

  test("Alias queries") {
    parse("select p.name,p.age from person as p") should 
      equal(Right(
        Select(List(Column("name", Some("p"), None), Column("age", Some("p"), None)),
               List(From(Table("person", Some("p")), Nil)), None, None, None, None)))

    parse("select p.name, p.age from person as p where p.age > 10") should 
      equal(Right(
        Select(List(Column("name", Some("p"), None), Column("age", Some("p"), None)),
               List(From(Table("person", Some("p")), Nil)), 
               Some(Where(Predicate(Column("age", Some("p"), None), Gt, Constant(typeOf[Long])))),
               None, None, None)))

    parse("select p.name,p.age from person p where p.age < 10 order by p.age") should 
      equal(Right(
        Select(List(Column("name", Some("p"), None), Column("age", Some("p"), None)),
               List(From(Table("person", Some("p")), Nil)),
               Some(Where(Predicate(Column("age", Some("p"), None), Lt, Constant(typeOf[Long])))),
               None, Some(OrderBy(List(Column("age", Some("p"), None)), None)), None)))

    parse("select p.name,p.age from person as p where p.age = 10 and p.name='joe' order by p.age asc") should 
      equal(Right(
        Select(List(Column("name", Some("p"),None), Column("age", Some("p"),None)),
               List(From(Table("person", Some("p")), Nil)), 
               Some(Where(And(Predicate(Column("age", Some("p"), None), Eq, Constant(typeOf[Long])),
                              Predicate(Column("name", Some("p"), None), Eq, Constant(typeOf[String]))))),
               None, Some(OrderBy(List(Column("age", Some("p"), None)), Some(Asc))), None)))

    parse("select name as personname from person") should
      equal(Right(
        Select(List(Column("name", None, Some("personname"))),
               List(From(Table("person", None), Nil)), None, None, None, None)))

    parse("select p.name as personname from person as p") should
      equal(Right(
        Select(List(Column("name", Some("p"), Some("personname"))),
               List(From(Table("person", Some("p")), Nil)), None, None, None, None)))
  }

/*
  test("Variable queries") {
    parse("select name, person.age from person where age > ?") should 
      equal(Right(Statement(List(Column("person", "age")), List(Column("person", "name"), Column("person", "age")))))

    parse("select name, person.age from person where person.age > ?") should 
      equal(Right(Statement(List(Column("person", "age")), List(Column("person", "name"), Column("person", "age")))))

    parse("select p.name, p.age from person as p where p.age > ?") should 
      equal(Right(Statement(List(Column("person", "age")), List(Column("person", "name"), Column("person", "age")))))

    parse("select p.name,p.age from person as p where p.age = ? and p.name=? order by p.age asc") should 
      equal(Right(Statement(List(Column("person", "age"), Column("person", "name")), List(Column("person", "name"), Column("person", "age")))))
  }

  test("Equi joins") {
    parse("select p.name, c.age from person as p, child c where p.name > c.parent") should 
      equal(Right(Statement(Nil, List(Column("person", "name"), Column("child", "age")))))

    parse("select p.name, c.age from person as p, child c where p.name > c.parent and c.id>?") should 
      equal(Right(Statement(List(Column("child", "id")), List(Column("person", "name"), Column("child", "age")))))
  }

  test("Joins") {
    parse("select p.name, j.name as employer, j.started, j.resigned from person p join job_history j on p.id=j.person") should
      equal(Right(Statement(Nil, List(Column("person", "name"), 
                                      Column("job_history", "name", Some("employer")), 
                                      Column("job_history", "started"), 
                                      Column("job_history", "resigned")))))

    parse("select j.name from person p inner join job_history j on p.id=j.person") should
      equal(Right(Statement(Nil, List(Column("job_history", "name")))))

    parse("select j.name from person p left outer join job_history j on p.id=j.person") should
      equal(Right(Statement(Nil, List(Column("job_history", "name")))))

    parse("select j.name from person p right outer join job_history j on p.id=j.person") should
      equal(Right(Statement(Nil, List(Column("job_history", "name")))))

    parse("select j.name from person p join job_history j on p.id=j.person where j.age>?") should
      equal(Right(Statement(List(Column("job_history", "age")), List(Column("job_history", "name")))))
  }
  */

  test("Functions") {
    parse("select name, AVG(price), SUM(price) as p from titles group by title having AVG(price) > ? and COUNT(price) > 100") should
      equal(Right(
        Select(List(Column("name", None, None), 
                    Function("AVG", List(Column("price", None, None)), None), 
                    Function("SUM", List(Column("price", None, None)), Some("p"))),
               List(From(Table("titles",None), Nil)), 
               None, 
               Some(GroupBy(Column("title", None, None), 
                            Some(Having(
                              And(
                                Predicate(
                                  Function("AVG", List(Column("price", None, None)), None), 
                                  Gt, 
                                  Input), 
                                Predicate(
                                  Function("COUNT", List(Column("price", None, None)), None), 
                                  Gt, 
                                  Constant(typeOf[Long]))))))),
               None, None)))
  }
}

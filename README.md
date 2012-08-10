Exploring techinques to embed SQL as an external DSL into Scala.
================================================================

Intro
-----

This experiment starts from following observations.

* The types and column names are already defined in the database schema or SQL query. Why not use those and infer types and accessor functions?

* SQL is a fine DSL for many queries. It is the native DSL of relational databases and wrapping it with another DSL is often unncessary (SQL sucks when one has to compose queries, or if you have to be database agnostic).

Examples
--------

First some boring initialization...

    import java.sql._
    import sqltyped._
    import Sql._
    Class.forName("com.mysql.jdbc.Driver")
    object Columns { object name; object age; object salary }
    implicit val c = Configuration("jdbc:mysql://localhost:3306/sqltyped", "com.mysql.jdbc.Driver", "root", "", Columns)
    val conn = DriverManager.getConnection(c.url, c.username, c.password)
    import Columns._

Now we are ready to query the data.

    scala> val q = sql("select name, age from person")
    scala> query(conn, q).map(p => p.get(age))
    res0: List[Int] = List(36, 14)

Notice how the type of 'age' was infered to be Int.

   scala> query(conn, q).map(p => p.get(age))
   <console>:24: error: No such column Columns.salary.type
                  query(conn, q).map(p => p.get(salary))

Oops, a compilation failure. Can't access 'salary', it was not selected in the query.

Status
------

This is a proof-of-concept currently.

The initial implementation uses [Scala macros](http://scalamacros.org) to connect to the database 
at compile time. The macro reads database schema and infers types and variable names from there. Query
results are returned as a type safe record. Those type safe records are emulated by building on
[Shapeless](https://github.com/milessabin/shapeless) HLists.

* Add support for Option (nullable columns)
* Add support for input parameters
* Tag primary keys?
* API to return tuples as well as associative HLists
* Full SQL syntax + SQL dialects 
* Requiring a user to create a type for each used column is unncessary boilerplate once Scala macros can create public types
* Benchmark the effect on compilation times and optimize as needed
* ...

How to try it?
--------------

    git clone https://github.com/jonifreeman/sqltyped.git
    cd sqltyped
    mysql -u root -e 'create database sqltyped'
    mysql -u root sqltyped < src/test/resources/test.sql
    sbt test // Requires >= 0.12.0 

Related
-------

* [PG'OCaml](http://pgocaml.forge.ocamlcore.org)
* [Slick](http://slick.typesafe.com)


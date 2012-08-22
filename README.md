Macro which infers Scala types from database
============================================

Intro
-----

This experiment starts from following observations.

* The types and column names are already defined in the database schema or SQL query. Why not use those and infer types and accessor functions?

* SQL is a fine DSL for many queries. It is the native DSL of relational databases and wrapping it with another DSL is often unncessary (SQL sucks when one has to compose queries, or if you have to be database agnostic).

Examples
--------

First some boring initialization... 

```sql
    create table person(name varchar(255) not null, age INT not null, salary INT not null);
    insert into person values ('joe', 36, 9500);
    insert into person values ('moe', 14, 8000);
```

Start console: ```sbt test:console```

```scala
    import java.sql._
    import sqltyped._
    Class.forName("com.mysql.jdbc.Driver")
    object Columns { object name; object age; object salary; object employer; object started; object resigned }
    implicit val c = Configuration(Columns)
    implicit def conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sqltyped", "root", "")
    import Columns._
```

Now we are ready to query the data.

```scala
    scala> val q = sql("select name, age from person")
    scala> q().map(p => p.get(age))
    res0: List[Int] = List(36, 14)
```

Notice how the type of 'age' was infered to be Int.

```scala
   scala> q().map(p => p.get(salary))
   <console>:24: error: No such column Columns.salary.type
                  query(conn, q).map(p => p.get(salary))
```

Oops, a compilation failure. Can't access 'salary', it was not selected in the query.

The query results are returned as a List of type safe records (think ```List[{name:String, age:Int}]```).
As the above examples showed a field of a record can be accessed with get function: ```row.get(name)```.
Functions ```values``` and ```tuples``` can be used to drop record names and get just the query values.

```scala
   scala> q().values
   res1: List[shapeless.::[String,shapeless.::[Int,shapeless.HNil]]] = List(joe :: 36 :: HNil, moe :: 14 :: HNil)

   scala> q().tuples
   res2: List[(String, Int)] = List((joe,36), (moe,14))
```

Input parameters are parsed and typed too.

```scala
    scala> val q = sql("select name from person where age > ?")

    scala> q("30").map(p => p.get(name))
    <console>:24: error: type mismatch;
     found   : String("30")
     required: Int
                  q("30").map(p => p.get(name))

    scala> q(30).map(p => p.get(name))
    res4: List[String] = List(joe)
```

Nullable columns are inferred to be Scala Option's.

```scala
    scala> val q = sql("""select p.name, j.name as employer, j.started, j.resigned 
                          from person p join job_history j on p.id=j.person order by employer""")
    scala> q().tuples
    res5: List[(String, String, java.sql.Timestamp, Option[java.sql.Timestamp])] = 
      List((joe,Enron,2002-08-02 12:00:00.0,Some(2004-06-22 18:00:00.0)), 
           (joe,IBM,2004-07-13 11:00:00.0,None))
```

Functions are supported too. Note how function 'max' is polymorphic on its argument. For String
column it is typed as String => String etc.

```scala
    scala> val q = sql("select max(name) as name, max(age) as age from person where age > ?")
    scala> q(10).tuples.head
    res6: (Option[String], Option[Int]) = (Some(moe),Some(36))
```

So far all the examples have returned results as Lists of records. But with a little bit of query
analysis we can do better. If just one column is selected it is unnecessary box the values as records.

```scala
    scala> sql("select name from person").apply
    res7: List[String] = List(joe, moe)

    scala> sql("select age from person").apply
    res8: List[Int] = List(36, 14)
```


Status
------

This is a proof-of-concept currently.

The initial implementation uses [Scala macros](http://scalamacros.org) to connect to the database 
at compile time. The macro reads database schema and infers types and variable names from there. Query
results are returned as type safe records. Those type safe records are emulated by building on
[Shapeless](https://github.com/milessabin/shapeless) HLists.

* Tag primary keys?
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


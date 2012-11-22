Demo app
========

Demo app is a small REST server. The stack is:

* Unfiltered (REST API)
* json4s (JSON rendering)
* sqlτyped (Database access)
* Shapeless (Record emulation)
* Slick (Database connection handling)
* MySQL (Database)

Note, the current version compiles but does not boot :) I couldn't find Scala 2.10 compatible Unfiltered binaries anywhere. Please ping me if you know where can I find those.

Breakdown
---------

### [schema.sql](https://github.com/jonifreeman/sqltyped/blob/master/demo/src/main/resources/schema.sql) ###

That's where the demo schema is defined. sqlτyped compiler will get an access to those definitions at compile time, which it then uses to infer Scala types of the SQL statements.

![Schema](http://yuml.me/d0e5d450)


### [package.scala](https://github.com/jonifreeman/sqltyped/blob/master/demo/src/main/scala/package.scala) ###

Database connection and sqlτyped is configured at package object. Fairly trivial stuff. Note, we have defined a type for each table ([tagging](https://github.com/jonifreeman/sqltyped/wiki/User-guide#wiki-tagging)) and for each column accessed by [record](https://github.com/jonifreeman/sqltyped/wiki/User-guide#wiki-records) syntax. The plan is to automatically generate those once Scala macros can create public members. 

### [testdata.scala](https://github.com/jonifreeman/sqltyped/blob/master/demo/src/main/scala/testdata.scala) ###

Initial testdata is created by executing SQL statements with function ```sqlk```. The SQL string literal is converted into a function by sqlτyped compiler. Why ```sqlk``` and not just ```sql```? Well, ```sqlk``` is a specialized version of function ```sql``` which returns generated keys instead of updated rows.

### [server.scala](https://github.com/jonifreeman/sqltyped/blob/master/demo/src/main/scala/server.scala) ###

REST endpoints are defined as Unfiltered pattern matches. The server is booted to port 8080 after test data is initialized. Note, some sloppy error handling but this is not an Unfiltered demo after all.

### [db.scala](https://github.com/jonifreeman/sqltyped/blob/master/demo/src/main/scala/server.scala) ###

This is where the meat of the demo is. sqlτyped promotes a style where SQL is used directly to define data access functions. It let's the programmer use all the available database features in a native form. It is a job of the compiler to integrate these two worlds as seamlessly as possible.

Note the complete lack of type annotations in defined ```sql``` functions. The types are inferred from database. If you know SQL, you know how to define data access functions with sqlτyped.

Processing results is slightly more involved and has some rough edges. sqlτyped returns results as list of [extensible records](https://github.com/jonifreeman/sqltyped/wiki/User-guide#wiki-records). Record system is built on top of HList which sometimes leaks through as very cryptic compiler error messages. Nevertheless, extensible record is a nice abstraction for database results and it is very easy to convert a record to a more familiar tuple when needed (```record.values.tupled```). Function ```personWithInterviews``` shows an example usage. ```personById``` returns a record:

```scala
{
  id: Long @@ Tables.person
, name: String
, interview: Option[Long @@ Tables.interview]
, rating: Option[Double]
, held_by: Option[String]
}
```

We could convert that directly to JSON with function ```sqltyped.json4s.JSON.compact```. However, to offer a nicer API some structure should be added to that flat row. We can modify the value of a field ```interview``` with function ```modify```. It is a function from the original value (of type ```Option[Long @@ Tables.interview]``` here) to a new value. The new value is a new record (record is a HList of key-value tuples). Fields which are added to the just created record are removed from the original record with function ```removeKey```.

```scala
personById(id) map { p =>
  p.modify(interview) { (i: Option[Long @@ Tables.interview]) =>
    (rating -> p.get(rating)) :: (held_by -> p.get(held_by)) :: ("comments" -> (i map comments.apply)) :: HNil
  } removeKey(rating) removeKey(held_by)
}
```

The result is a following record which can be directly rendered as a nice JSON document.

```scala
{
  id: Long @@ Tables.person
, name: String
, interview: {
    rating: Option[Double]
  , held_by: Option[String]
  , comments: List[{text: String, created: Timestamp, author: String}]
  }
}
```

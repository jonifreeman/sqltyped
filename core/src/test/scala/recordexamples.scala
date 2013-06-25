package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

object JSON {
  object toJSON extends Poly1 {
    implicit def numToJSON[V <% Number] = at[V](_.toString)
    implicit def stringToJSON[V <% String] = at[V]("\"" + _.toString + "\"")
    implicit def boolToJSON[V <% Boolean] = at[V](_.toString)
    implicit def dateToJSON[V <% java.util.Date] = at[V]("\"" + _.toString + "\"")

    implicit def seqToJSON[V, L <% Seq[V]](implicit c: Case1[V]) = 
      at[L](_.map(v => toJSON(v)).mkString("[", ",", "]"))

    implicit def recordToJSON[R <: HList](implicit foldMap: MapFolder[R, String, fieldToJSON.type]) = {
      val concat = (s1: String, s2: String) => if (s2 != "") s1 + "," + s2 else s1

      at[R](r => "{" + (r.foldMap("")(fieldToJSON)(concat)) + "}")
    }

    object fieldToJSON extends Poly1 {
      implicit def value[K, V](implicit c: toJSON.Case1[V]) = at[(K, V)] {
        case (k, v) => "\"" + keyAsString(k) + "\":" + toJSON(v)
      }

      implicit def option[K, V](implicit c: Case1[(K, V)]) = at[(K, Option[V])] { 
        case (k, Some(v)) => fieldToJSON((k, v)).toString
        case (k, None) => ""
      } 
    }
  }
}

class RecordExampleSuite extends MySQLConfig {
  test("Map over record elements") {
    val joe = sql("select * from person where id=?").apply(1) getOrElse sys.error("No data")

    object asString extends Poly1 {
      implicit def anyToString[K, V] = at[(K, V)] { 
        case (k, v) => keyAsString(k) + "=" + v.toString 
      }
    }
    
    (joe map asString) === "id=1" :: "name=joe" :: "age=36" :: "salary=9500" :: "img=None" :: HNil
  }

  test("Query to JSON") {
    val rows = sql("select id, name as fname, age from person limit 100").apply

    JSON.toJSON(rows) === """[{"id":1,"fname":"joe","age":36},{"id":2,"fname":"moe","age":14}]"""
  }

  test("Modify a record") {
    val joe = sql("select id, name from person where id=?").apply(1) getOrElse sys.error("No data")

    // Add a field
    object lname
    val joe2 = (lname -> "Doe") :: joe
    joe2.get(lname) === "Doe"

    // Remove by a key
    joe2.removeKey(lname) === joe
    
    // Modify a field
    val joe3 = joe2.modify(lname)((s: String) => s.length)
    joe3.get(lname) === 3

    // Rename a field
    object lastname
    val joe4 = joe2.renameKey(lname, lastname)
    joe4.get(lastname) === "Doe"
  }

  test("Query to CSV") {
    val rows = sql("select name as fname, age, img from person limit 100").apply.values

    CSV.fromList(rows) === """ "joe","36",""
                              |"moe","14","" """.stripMargin.trim
  }

  test("Query to untyped tuples") {
    val rows = sql("select name, age from person limit 100").apply
    Record.toTupleList(rows.head) === List(("name", "joe"), ("age", 36))
    Record.toTupleLists(rows) === 
      List(List(("name", "joe"), ("age", 36)), List(("name", "moe"), ("age", 14))) 
  }

/*
  test("Query to a case class") {
  }
  
  test("Merge records") {
  }
*/
}


package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

object JSON {
  object fieldToJSON extends Poly1 {
    implicit def numToJSON[K, V <% Number] = at[(K, V)] { 
      case (k, v) => "\"" + keyAsString(k) + "\":" + v.toString 
    }
    implicit def stringToJSON[K, V <% String] = at[(K, V)] { 
      case (k, v) => "\"" + keyAsString(k) + "\":\"" + v + "\""
    }
    implicit def boolToJSON[K, V <% Boolean] = at[(K, V)] { 
      case (k, v) => "\"" + keyAsString(k) + "\":" + v.toString 
    }
    implicit def tstampToJSON[K, V <% java.sql.Timestamp] = at[(K, V)] { 
      case (k, v) => "\"" + keyAsString(k) + "\":\"" + v.toString + "\""
    }
    implicit def optionToJSON[K, V](implicit c: Case1[(K, V)]) = at[(K, Option[V])] { 
      case (k, Some(v)) => fieldToJSON((k, v)).toString
      case (k, None) => ""
    }
  }

  def toJSON[R <: HList](record: R)(implicit foldMap: MapFolder[R, String, fieldToJSON.type]): String = {
    val concat = (s1: String, s2: String) => if (s2 != "") s1 + "," + s2 else s1

    "{" + (record.foldMap("")(fieldToJSON)(concat)) + "}"
  }

  def toJSON[R <: HList](records: List[R])(implicit foldMap: MapFolder[R, String, fieldToJSON.type]): String = 
    (records map (r => toJSON(r))).mkString("[", ",", "]")
}

class RecordExampleSuite extends Example {
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

/*
  test("Query to a case class") {
  }
  
  test("Merge records") {
  }
*/
}


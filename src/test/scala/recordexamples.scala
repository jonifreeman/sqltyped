package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

object JSON {
  object fieldToJSON extends Poly1 {
    implicit def longToJSON[K] = at[(K, Long)] { 
      case (k, v) => "\"" + keyAsString(k) + "\":" + v.toString 
    }
    implicit def intToJSON[K] = at[(K, Int)] { 
      case (k, v) => "\"" + keyAsString(k) + "\":" + v.toString 
    }
    implicit def stringToJSON[K] = at[(K, String)] { 
      case (k, v) => "\"" + keyAsString(k) + "\":\"" + v + "\""
    }
    implicit def pkToJSON[K, T] = at[(K, Long @@ T)] { 
      case (k, v) => "\"" + keyAsString(k) + "\":" + v.toString 
    }    
  }

  def toJSON[R <: HList](rows: List[R])(implicit foldMap: MapFolder[R, String, fieldToJSON.type]) = {
    val concat = (s1: String, s2: String) => if (s2 != "") s1 + "," + s2 else s1

    (rows map (row => "{" + (row.foldMap("")(fieldToJSON)(concat)) + "}")).mkString("[", ",", "]")
  }
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


package sqltyped

import java.sql._
import org.scalatest._
import shapeless._

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

/*
  test("Modify a record") {
  }
  
  test("Query to case class") {
  }

  test("Query to JSON") {
    import JSON._

    val q = sql("select * from person limit 100").apply
    toJSON(q) === """ [{},{}] """
  }
*/
}


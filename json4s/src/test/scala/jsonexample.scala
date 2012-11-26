package sqltyped.json4s

import org.scalatest._
import shapeless._
import sqltyped._

class JSONExampleSuite extends FunSuite with matchers.ShouldMatchers {
  test("Record to JSON") {
    trait SomeTag
    object street; object city; object name; object toys; object age; object address; object children

    val addr = (street -> "Boulevard") :: (tag[SomeTag](city) -> "Helsinki") :: HNil
    val child1 = (name -> "ella") :: (toys -> List("paperdoll", "jump rope")) :: (age -> (Some(4): Option[Int])) :: HNil
    val child2 = (name -> "moe") :: (toys -> List("tin train")) :: (age -> (None: Option[Int])) :: HNil
    val person = (name -> "joe") :: (age -> 36) :: (address -> addr) :: (children -> Seq(child1, child2)) :: HNil

    JSON.compact(addr) should equal("""{"street":"Boulevard","city":"Helsinki"}""")
    
    JSON.compact(child1) should equal("""{"name":"ella","toys":["paperdoll","jump rope"],"age":4}""")

    JSON.compact(person) should equal("""{"name":"joe","age":36,"address":{"street":"Boulevard","city":"Helsinki"},"children":[{"name":"ella","toys":["paperdoll","jump rope"],"age":4},{"name":"moe","toys":["tin train"]}]}""")
  }

  test("Nulls") {
    object key1; object key2
    val foo: java.util.Date = null
    val bad = (key1 -> null) :: (key2 -> foo) :: HNil
    JSON.compact(bad) should equal("""{"key1":null,"key2":null}""")
  }
}

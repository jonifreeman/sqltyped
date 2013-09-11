package sqltyped.json4s

import org.scalatest._
import shapeless._, ops.record._, syntax.singleton._
import sqltyped._

class JSONExampleSuite extends FunSuite with matchers.ShouldMatchers {
  test("Record to JSON") {
    val SomeTag = Witness("SomeTag")

    val addr = ("street" ->> "Boulevard") :: ("city" ->> tag[SomeTag.T]("Helsinki")) :: HNil
    val child1 = ("name" ->> "ella") :: ("toys" ->> List("paperdoll", "jump rope")) :: ("age" ->> (Some(4): Option[Int])) :: HNil
    val child2 = ("name" ->> "moe") :: ("toys" ->> List("tin train")) :: ("age" ->> (None: Option[Int])) :: HNil
    val person = ("name" ->> "joe") :: ("age" ->> 36) :: ("address" ->> addr) :: ("children" ->> Seq(child1, child2)) :: HNil

    JSON.compact(addr) should equal("""{"street":"Boulevard","city":"Helsinki"}""")
    
    JSON.compact(child1) should equal("""{"name":"ella","toys":["paperdoll","jump rope"],"age":4}""")

    JSON.compact(person) should equal("""{"name":"joe","age":36,"address":{"street":"Boulevard","city":"Helsinki"},"children":[{"name":"ella","toys":["paperdoll","jump rope"],"age":4},{"name":"moe","toys":["tin train"],"age":null}]}""")
  }

/*
  test("Nulls") {
    val foo: String = null
    val bad = ("key1" ->> null) :: ("key2" ->> foo) :: HNil
    JSON.compact(bad) should equal("""{"key1":null,"key2":null}""")
  }
*/

  test("Date") {
    implicit val formats = org.json4s.DefaultFormats
    val p = ("name" ->> "Joe") :: ("birthdate" ->> new java.util.Date(0)) :: HNil
    JSON.compact(p) should equal("""{"name":"Joe","birthdate":"1970-01-01T00:00:00Z"}""")
  }

  test("Data which is already in JSON format") {
    import org.json4s._

    val p = ("name" ->> "Joe") :: ("addr" ->> JObject(List("street" -> JString("Boulevard"), "city" -> JString("Helsinki")))) :: HNil
    JSON.compact(p) should equal("""{"name":"Joe","addr":{"street":"Boulevard","city":"Helsinki"}}""")
  }
}

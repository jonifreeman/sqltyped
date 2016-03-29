package sqltyped.json4s

import org.json4s._
import native.JsonMethods
import JsonMethods._

import shapeless._
import labelled._
import poly._
import ops.hlist._
import syntax.singleton._
import record._
import shapeless.tag.@@

/**
 * Note, copy-pasted from https://github.com/rrmckinley/jsonless
 *
 * Remove this once 'jsonless' is published to Sonatype repos.
 */

object Record {
  def keyAsString[F, V](f: FieldType[F, V])(implicit wk: shapeless.Witness.Aux[F]) =
    wk.value.toString
}

object JSON {
  def compact[A](a: A)(implicit ctoj: toJSON.Case[A] { type Result = JValue }): String = 
    JsonMethods.compact(render(ctoj(a)))

  def pretty[A](a: A)(implicit ptoj: toJSON.Case[A] { type Result = JValue }): String = 
    JsonMethods.pretty(render(ptoj(a)))
}

object toJSON extends Poly1 {
  implicit def atNull = at[Null](_ => JNull : JValue)
  implicit def atDouble = at[Double](JDouble(_) : JValue)
  implicit def atBigInt = at[BigInt](JInt(_) : JValue)
  implicit def atBigDecimal = at[BigDecimal](JDecimal(_) : JValue)
  implicit def atNumber[V <% Long] = at[V](i => JInt(BigInt(i)) : JValue)
  implicit def atString = at[String](s => (if (s == null) JNull else JString(s)) : JValue)
  implicit def atBoolean = at[Boolean](JBool(_) : JValue)
  implicit def atJSON[V <: JValue] = at[V](json => json : JValue)

  implicit def atTagged[V, T](implicit ttoj: toJSON.Case[V] { type Result = JValue }) =
    at[V @@ T](v => ttoj(v: V) : JValue)

  implicit def atOption[V](implicit otoj: toJSON.Case[V] { type Result = JValue }) =
    at[Option[V]] {
      case Some(v) => otoj(v : V) : JValue
      case None    => JNull : JValue
    }
  
  implicit def atDate[V <: java.util.Date](implicit f: Formats) =
    at[V](s => (if (s == null) JNull else JString(f.dateFormat.format(s))) : JValue)

  implicit def atTraversable[V, C[V] <: Traversable[V]](implicit ttoj: toJSON.Case[V] { type Result = JValue }) =
    at[C[V]](l => JArray(l.toList.map(v => ttoj(v : V) : JValue)) : JValue)

  implicit def atMap[K, V](implicit mtoj: toJSON.Case[V] { type Result = JValue }) =
    at[Map[K, V]](m => JObject(m.toList.map(e => JField(e._1.toString, mtoj(e._2 : V) : JValue))) : JValue)

  implicit def atRecord[R <: HList, F, V](implicit folder: MapFolder[R, List[JField], fieldToJSON.type]) =
    at[R](r => JObject(r.foldMap(Nil: List[JField])(fieldToJSON)(_ ::: _)) : JValue)
}

object fieldToJSON extends Poly1 {
  implicit def atFieldType[F, V](implicit ftoj: toJSON.Case[V] { type Result = JValue }, wk: shapeless.Witness.Aux[F]) = at[FieldType[F, V]] {
    f => (Record.keyAsString(f), ftoj(f : V) : JValue) :: Nil
  }
}

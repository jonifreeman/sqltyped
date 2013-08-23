package sqltyped

import shapeless._, ops.hlist._, tag.@@

trait Show[A] {
  def show(a: A): String
}

object Show {
  implicit object ShowByte extends ToStringShow[Byte]
  implicit object ShowInt extends ToStringShow[Int]
  implicit object ShowShort extends ToStringShow[Short]
  implicit object ShowDouble extends ToStringShow[Double]
  implicit object ShowLong extends ToStringShow[Long]
  implicit object ShowFloat extends ToStringShow[Float]
  implicit object ShowString extends ToStringShow[String]
  implicit object ShowBool extends ToStringShow[Boolean]
  implicit def ShowOption[A]: Show[Option[A]] = new Show[Option[A]] {
    def show(a: Option[A]) = ""
  }
  implicit def ShowTagged[A: Show, T]: Show[A @@ T] = new Show[A @@ T] {
    def show(a: A @@ T) = implicitly[Show[A]].show(a: A)
  }

  class ToStringShow[A] extends Show[A] {
    def show(a: A) = a.toString
  }
}

object CSV {
  def fromList[R <: HList](rs: List[R], separator: String = ",")(implicit foldMap: MapFolder[R, List[String], toCSV.type]) =
    (rs map (r => fromRow(r, separator))).mkString("\n")

  def fromRow[R <: HList](r: R, separator: String = ",")(implicit foldMap: MapFolder[R, List[String], toCSV.type]) =
    (columnsFromRow(r) map escape).mkString(separator)

  def columnsFromRow[R <: HList](r: R)(implicit foldMap: MapFolder[R, List[String], toCSV.type]) =
    r.foldMap(Nil: List[String])(toCSV)(_ ::: _)

  private def escape(s: String) = "\"" + s.replaceAll("\"","\"\"") + "\""
}

object toCSV extends Poly1 {
  implicit def valueToCsv[V: Show] = at[V](v => List(implicitly[Show[V]].show(v)))
}

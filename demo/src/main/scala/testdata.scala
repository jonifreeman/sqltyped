package demo

import sqltyped._

object TestData {
  def drop = sql("delete from person").apply

  def create = {
    val newPerson = sqlk("insert into person (name, secret, interview) values (?, ?, ?)")
    val admin = newPerson("Admin", None, None)
    val other = newPerson("Some other guy", None, None)
    val interview = sqlk("insert into interview (held_by, rating) values (?, ?)").apply(admin, Some(4.5))
    val dick = newPerson("Dick Tracy", Some("secret"), Some(interview))
    Db.newComment("My first comment.", admin, dick)
    Db.newComment("My second comment.", other, dick)
  }
}

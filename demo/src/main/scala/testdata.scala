package demo

import sqltyped._

object TestData {
  def drop = sql("DELETE FROM person").apply

  def create = {
    val newPerson = sqlk("INSERT INTO person (name, secret, interview) VALUES (?, ?, ?)")
    val admin = newPerson("Admin", None, None)
    val other = newPerson("Some other guy", None, None)
    val interview = sqlk("INSERT INTO interview (held_by, rating) VALUES (?, ?)").apply(admin, Some(4.5))
    val dick = newPerson("Dick Tracy", Some("secret"), Some(interview))
    Db.newComment("My first comment.", admin, dick)
    Db.newComment("My second comment.", other, dick)
  }
}

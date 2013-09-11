package demo

import sqltyped._
import shapeless._

object Db {
  val personNames    = sql("SELECT name FROM person")
  val personIdByName = sql("SELECT id FROM person WHERE name=?")

  val personById = 
    sql("""SELECT p.id, p.name, p.interview, i.rating, p2.name AS held_by
           FROM person p LEFT JOIN interview i ON p.interview=i.id
                         LEFT JOIN person p2 ON i.held_by=p2.id
           WHERE p.id = ? LIMIT 1""")

  val comments = 
    sql("""SELECT c.text, c.created, p.name AS author
           FROM comment c JOIN person p ON c.author=p.id
           WHERE c.interview = ?""")

  val newComment =
    sql("""INSERT INTO comment (text, created, author, interview)
           SELECT ?, now(), ?, p.interview FROM person p 
           WHERE p.id = ? LIMIT 1""")

  def personWithInterviews(id: Long) = personById(id) map { p =>
    p.updateWith("interview") { _ map (i =>
      "rating" ->> p.get("rating") :: "held_by" ->> p.get("held_by") :: "comments" ->> comments(i) :: HNil
    )} - "rating" - "held_by"
  }
}

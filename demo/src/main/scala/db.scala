package demo

import sqltyped._
import shapeless._
import Columns._

object Db {
  val personNames    = sql("SELECT name FROM person")
  val personIdByName = sql("SELECT id from person WHERE name=?")

  val personById = 
    sql("""SELECT p.id, p.name, p.interview, i.rating, p2.name as held_by
           FROM person p left join interview i on (p.interview=i.id)
                         left join person p2 on (i.held_by=p2.id)
           WHERE p.id = ? LIMIT 1""")

  val comments = 
    sql("""SELECT c.text, c.created, p.name as author
           FROM comment c join person p on (c.author=p.id)
           WHERE c.interview = ?""")

  val newComment =
    sql("""INSERT INTO comment (text, created, author, interview)
           SELECT ?, now(), ?, p.interview FROM person p 
           WHERE p.id = ? LIMIT 1""")

  def personWithInterviews(id: Long) = personById(id) map { p =>
    p.modify(interview) { (i: Option[Long @@ Tables.interview]) => 
      (rating -> p.get(rating)) :: (held_by -> p.get(held_by)) :: ("comments" -> (i map comments.apply)) :: HNil
    } removeKey(rating) removeKey(held_by)
  }
}

package demo

import unfiltered.request._
import unfiltered.response._
import sqltyped.json4s.JSON.compact

object Server extends scala.App {
  val api = unfiltered.netty.cycle.Planify {
    case GET(Path("/people")) =>
      Ok ~> Json(compact(Db.personNames.apply))

    case GET(Path(Seg("person" :: id :: Nil))) =>
      Db.personWithInterviews(id.toLong).headOption match {
        case None => NotFound ~> ResponseString("No such person")
        case Some(p) => Ok ~> Json(compact(p))
      }

    case PUT(Path(Seg("person" :: id :: "comment" :: Nil)) & Params(params)) =>
      val author = Db.personIdByName("Admin") getOrElse sys.error("Initialization error: no admin")
      Db.newComment(params("text").head, author, id.toLong)
      Ok ~> ResponseString("OK")
  }

  db.withSession { TestData.drop; TestData.create }
  unfiltered.netty.Http(8080)
    .plan(unfiltered.netty.cycle.Planify { case x => db.withSession(api.intent(x)) })
    .run(svr => println("Running: " + svr.url), svr => println("Shutting down."))

  def Json(s: String) = JsonContent ~> ResponseString(s)
}

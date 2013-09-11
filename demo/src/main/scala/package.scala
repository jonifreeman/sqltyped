import scala.slick.session.Database
import sqltyped._

package object demo {
  Class.forName("com.mysql.jdbc.Driver")
  val db = Database.forURL("jdbc:mysql://localhost:3306/sqltyped_demo", 
                           driver = "com.mysql.jdbc.Driver", user = "root", password = "")

  implicit val formats = org.json4s.DefaultFormats
  implicit def conn = Database.threadLocalSession.conn
}

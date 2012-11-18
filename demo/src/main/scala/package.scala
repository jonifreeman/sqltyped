import scala.slick.session.Database
import sqltyped._

package object demo {
  Class.forName("com.mysql.jdbc.Driver")
  object Tables { trait person; trait interview; trait comment }
  object Columns { object interview; object rating; object held_by; }
  val db = Database.forURL("jdbc:mysql://localhost:3306/sqltyped_demo", 
                           driver = "com.mysql.jdbc.Driver", user = "root", password = "")

  implicit val config = Configuration(Tables, Columns)
  implicit def conn = Database.threadLocalSession.conn
}

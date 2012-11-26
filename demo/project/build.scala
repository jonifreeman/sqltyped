import sbt._
import Keys._

object DemoBuild extends Build {
  lazy val demoSettings = Defaults.defaultSettings ++ Seq(
    organization := "com.example",
    version := "0.1",
    scalaVersion := "2.10.0-RC3",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),
    crossPaths := false,
    libraryDependencies ++= Seq(
      "fi.reaktor" % "sqltyped_2.10.0-RC3" % "0.1-SNAPSHOT",
      "fi.reaktor" % "sqltyped-json4s_2.10.0-RC3" % "0.1-SNAPSHOT",
      "com.typesafe" % "slick_2.10.0-RC3" % "0.11.2",
      "net.databinder" % "unfiltered-filter_2.9.2" % "0.6.4",
      "net.databinder" % "unfiltered-jetty_2.9.2" % "0.6.4",
      "mysql" % "mysql-connector-java" % "5.1.21"
    ),
    initialize ~= { _ => initSqltyped },
    resolvers ++= Seq(sonatypeNexusSnapshots, sonatypeNexusReleases)
  )

  lazy val demo = Project(
    id = "sqltyped-demo",
    base = file("."),
    settings = demoSettings
  )

  def initSqltyped {
    System.setProperty("sqltyped.url", "jdbc:mysql://localhost:3306/sqltyped_demo")
    System.setProperty("sqltyped.driver", "com.mysql.jdbc.Driver")
    System.setProperty("sqltyped.username", "root")
    System.setProperty("sqltyped.password", "")
  }
  
  val sonatypeNexusSnapshots = "Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  val sonatypeNexusReleases = "Sonatype Nexus Releases" at "https://oss.sonatype.org/content/repositories/releases"
}

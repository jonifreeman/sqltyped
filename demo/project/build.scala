import sbt._
import Keys._

object DemoBuild extends Build {
  lazy val demoSettings = Defaults.defaultSettings ++ Seq(
    organization := "com.example",
    version := "0.4.1",
    scalaVersion := "2.11.7",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),
    crossPaths := false,
    libraryDependencies ++= Seq(
      "fi.reaktor" %% "sqltyped" % "0.4.1",
      "fi.reaktor" %% "sqltyped-json4s" % "0.4.0",
      "com.typesafe" %% "slick" % "1.0.0-RC1",
      "net.databinder" %% "unfiltered" % "0.6.5",
      "net.databinder" %% "unfiltered-netty" % "0.6.5",
      "net.databinder" %% "unfiltered-netty-server" % "0.6.5",
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

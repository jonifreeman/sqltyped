import sbt._
import Keys._

object SqltypedBuild extends Build with Publish {
  import Resolvers._

  lazy val majorVersion = "0.1"

  lazy val sqltypedSettings = Defaults.defaultSettings ++ publishSettings ++ Seq(
    organization := "fi.reaktor",
    version := "%s-SNAPSHOT" format majorVersion,
    scalaVersion := "2.10.0-RC2",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),
    crossVersion := CrossVersion.full,
    crossScalaVersions := Seq("2.10.0-RC2"),
    parallelExecution in Test := false,
    resolvers ++= Seq(sonatypeNexusSnapshots, sonatypeNexusReleases)
  )

  lazy val root = Project("root", file(".")) aggregate(core)

  lazy val core = Project(
    id = "sqltyped",
    base = file("core"),
    settings = sqltypedSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.chuusai" % "shapeless_2.10.0-RC2" % "1.2.3-SNAPSHOT",
        "org.scala-lang" % "scala-reflect" % "2.10.0-RC2",
        "net.sourceforge.schemacrawler" % "schemacrawler" % "8.17",
        "org.scalatest" % "scalatest_2.10.0-RC1" % "2.0.M4-2.10.0-RC1-B1" % "test",
        "org.scala-lang" % "scala-actors" % "2.10.0-RC2" % "test",
        "mysql" % "mysql-connector-java" % "5.1.21" % "test",
        "postgresql" % "postgresql" % "9.1-901.jdbc4" % "test"
      ),
      initialize ~= { _ => initSqltyped }
    )
  )

  lazy val slickIntegration = Project(
    id = "sqltyped-slick",
    base = file("slick-integration"),
    settings = sqltypedSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.typesafe" % "slick_2.10.0-M7" % "0.11.1"
      ),
      initialize ~= { _ => initSqltyped }
    )
  ) dependsOn(core % "compile;test->test;provided->provided")

  def initSqltyped {
/*
        System.setProperty("sqltyped.url", "jdbc:postgresql://localhost/sqltyped")
        System.setProperty("sqltyped.driver", "org.postgresql.Driver")
        System.setProperty("sqltyped.username", "sqltypedtest")
        System.setProperty("sqltyped.password", "secret") */
    System.setProperty("sqltyped.url", "jdbc:mysql://localhost:3306/sqltyped")
    System.setProperty("sqltyped.driver", "com.mysql.jdbc.Driver")
    System.setProperty("sqltyped.username", "root")
    System.setProperty("sqltyped.password", "")
  }
  
  object Resolvers {
    val sonatypeNexusSnapshots = "Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
    val sonatypeNexusReleases = "Sonatype Nexus Releases" at "https://oss.sonatype.org/content/repositories/releases"
  }
}

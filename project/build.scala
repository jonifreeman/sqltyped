import sbt._
import Keys._

object SqltypedBuild extends Build with Publish {
  import Resolvers._

//lazy val versionFormat = "%s"
  lazy val majorVersion = "0.4.2"
  lazy val versionFormat = "%s-SNAPSHOT"

  lazy val sqltypedSettings = Defaults.defaultSettings ++ publishSettings ++ Seq(
    organization := "fi.reaktor",
    version := versionFormat format majorVersion,
    scalaVersion := "2.11.7",
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),
    crossScalaVersions := Seq("2.11"),
    parallelExecution in Test := false,
    resolvers ++= Seq(sonatypeNexusSnapshots, sonatypeNexusReleases)
  )

  lazy val root = Project("root", file(".")) aggregate(core)

  lazy val core = Project(
    id = "sqltyped",
    base = file("core"),
    settings = sqltypedSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.chuusai" %% "shapeless" % "2.3.0",
        "net.sourceforge.schemacrawler" % "schemacrawler" % "8.17",
        "org.scala-lang" % "scala-reflect" % "2.11.7",
        "org.scalatest" %% "scalatest" % "2.2.6" % "test",
        "org.scala-lang" % "scala-actors" % "2.11.7" % "test",
        "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
        "mysql" % "mysql-connector-java" % "5.1.21" % "test",
        "postgresql" % "postgresql" % "9.1-901.jdbc4" % "test"
      ),
      initialize ~= { _ => initSqltyped }
    )
  )

  lazy val json4s = Project(
    id = "sqltyped-json4s",
    base = file("json4s"),
    settings = sqltypedSettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.json4s" %% "json4s-native" % "3.3.0"
      )
    )
  ) dependsOn(core % "compile;test->test;provided->provided")

  lazy val slickIntegration = Project(
    id = "sqltyped-slick",
    base = file("slick-integration"),
    settings = sqltypedSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.typesafe.slick" %% "slick" % "3.1.1"
      ),
      initialize ~= { _ => initSqltyped }
    )
  ) dependsOn(core % "compile;test->test;provided->provided")

  def initSqltyped {
    System.setProperty("sqltyped.url", "jdbc:mysql://localhost:3306/sqltyped")
    System.setProperty("sqltyped.driver", "com.mysql.jdbc.Driver")
    System.setProperty("sqltyped.username", "root")
    System.setProperty("sqltyped.password", "")

    System.setProperty("sqltyped.postgresql.url", "jdbc:postgresql://localhost/sqltyped")
    System.setProperty("sqltyped.postgresql.driver", "org.postgresql.Driver")
    System.setProperty("sqltyped.postgresql.username", "sqltypedtest")
    System.setProperty("sqltyped.postgresql.password", "secret")
    System.setProperty("sqltyped.postgresql.schema", "sqltyped")
  }
  
  object Resolvers {
    val sonatypeNexusSnapshots = "Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
    val sonatypeNexusReleases = "Sonatype Nexus Releases" at "https://oss.sonatype.org/content/repositories/releases"
  }
}

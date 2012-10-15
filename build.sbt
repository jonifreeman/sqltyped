organization := "sqltyped"

name := "sqltyped"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.0-RC1"

scalacOptions ++= Seq("-unchecked", "-deprecation")

parallelExecution in Test := false

resolvers ++= Seq(
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies ++= Seq(
  "com.chuusai" % "shapeless_2.10.0-RC1" % "1.2.3-SNAPSHOT",
  "org.scala-lang" % "scala-reflect" % "2.10.0-RC1",
  "net.sourceforge.schemacrawler" % "schemacrawler" % "8.17",
  "org.scalatest" % "scalatest_2.10.0-RC1" % "2.0.M4-2.10.0-RC1-B1" % "test",
  "org.scala-lang" % "scala-actors" % "2.10.0-RC1" % "test",
  "mysql" % "mysql-connector-java" % "5.1.21" % "test",
  "postgresql" % "postgresql" % "9.1-901.jdbc4" % "test"
)

initialize ~= { _ =>
//  System.setProperty("sqltyped.url", "jdbc:postgresql://localhost/sqltyped")
//  System.setProperty("sqltyped.driver", "org.postgresql.Driver")
//  System.setProperty("sqltyped.username", "sqltypedtest")
//  System.setProperty("sqltyped.password", "secret")
  System.setProperty("sqltyped.url", "jdbc:mysql://localhost:3306/sqltyped")
  System.setProperty("sqltyped.driver", "com.mysql.jdbc.Driver")
  System.setProperty("sqltyped.username", "root")
  System.setProperty("sqltyped.password", "")
}

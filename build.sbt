organization := "sqltyped"

name := "sqltyped"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.0-M6"

scalacOptions ++= Seq("-unchecked", "-deprecation")

parallelExecution in Test := false

resolvers ++= Seq(
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies ++= Seq(
  "com.chuusai" % "shapeless_2.10.0-M6" % "1.2.3-SNAPSHOT",
  "org.scala-lang" % "scala-reflect" % "2.10.0-M6",
  "net.sourceforge.schemacrawler" % "schemacrawler" % "8.17",
  "org.scalatest" % "scalatest_2.10.0-M6" % "1.9-2.10.0-M6-B2" % "test",
  "mysql" % "mysql-connector-java" % "5.1.21" % "test"
)

initialize ~= { _ =>
  System.setProperty("sqltyped.url", "jdbc:mysql://localhost:3306/sqltyped")
  System.setProperty("sqltyped.driver", "com.mysql.jdbc.Driver")
  System.setProperty("sqltyped.username", "root")
  System.setProperty("sqltyped.password", "")
}

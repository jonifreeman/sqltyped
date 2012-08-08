organization := "sqltyped"

name := "sqltyped"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.0-M6"

resolvers ++= Seq(
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies ++= Seq(
  "com.chuusai" % "shapeless_2.10.0-M6" % "1.2.3-SNAPSHOT",
  "org.scala-lang" % "scala-reflect" % "2.10.0-M6"
)


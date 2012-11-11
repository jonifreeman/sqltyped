import sbt._
import Keys._

trait Publish {
  val nexus = "https://oss.sonatype.org/"
  val snapshots = "snapshots" at nexus + "content/repositories/snapshots"
  val releases  = "releases" at nexus + "service/local/staging/deploy/maven2"

  lazy val publishSettings = Seq(
    publishMavenStyle := true,
    publishTo <<= version((v: String) => Some(if (v.trim endsWith "SNAPSHOT") snapshots else releases)),
    publishArtifact in Test := false,
    pomIncludeRepository := (_ => false),
    pomExtra := projectPomExtra
  )

  val projectPomExtra = 
    <url>https://github.com/jonifreeman/sqltyped</url>
    <licenses>
      <license>
        <name>Apache License</name>
        <url>http://www.apache.org/licenses/</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:jonifreeman/sqltyped.git</url>
      <connection>scm:git:git@github.com:jonifreeman/sqltyped.git</connection>
    </scm>
    <developers>
      <developer>
        <id>jonifreeman</id>
        <name>Joni Freeman</name>
        <url>https://twitter.com/jonifreeman</url>
      </developer>
    </developers>
}

name := "aws-kinesis-scala"

organization := "jp.co.bizreach"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-kinesis" % "1.10.4"
)

scalacOptions := Seq("-deprecation", "-feature")

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
  else                             Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

publishMavenStyle := true

pomExtra := (
  <url>https://github.com/bizreach/dynamodb4s</url>
    <licenses>
      <license>
        <name>The Apache Software License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <url>https://github.com/bizreach/aws-kinesis-scala</url>
      <connection>scm:git:https://github.com/bizreach/aws-kinesis-scala.git</connection>
    </scm>
    <developers>
      <developer>
        <id>takezoe</id>
        <name>Naoki Takezoe</name>
        <email>naoki.takezoe_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
    </developers>)

name := "aws-kinesis-scala"

lazy val commonSettings = Seq(
  organization := "jp.co.bizreach",
  scalaVersion := "2.11.12",
  crossScalaVersions := Seq(scalaVersion.value, "2.12.5"),
  scalacOptions ++= Seq("-feature", "-deprecation")
)

lazy val root = (project in file("."))
  .aggregate(core, spark)
  .settings(commonSettings: _*)
  .settings(
    packagedArtifacts := Map.empty
  )

lazy val core = project
  .settings(commonSettings: _*)
  .settings(
    name := "aws-kinesis-scala",
    libraryDependencies ++= Seq(
      "com.amazonaws" %  "aws-java-sdk-kinesis" % "1.11.311",
      "org.slf4j"     %  "slf4j-api"            % "1.7.25",
      "org.scalatest" %% "scalatest"            % "3.0.5" % "test"
    )
  )

lazy val spark = project
  .settings(commonSettings: _*)
  .settings(
    name := "aws-kinesis-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.11" % "2.3.0" % "provided"
    )
  ).dependsOn(core)


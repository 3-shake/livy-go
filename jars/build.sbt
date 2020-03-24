import java.io.File
import scala.sys.process._
import scala.reflect.runtime.universe._

// general config
ThisBuild / organization := "com.example"
ThisBuild / version := "1.0.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / parallelExecution := false
Global / concurrentRestrictions := Seq(Tags.limitAll(1))

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark"  %% "spark-core"    % "2.4.4" % "provided",
    "org.apache.spark"  %% "spark-sql"     % "2.4.4" % "provided",
    "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test
  ),
  test in assembly := {},
  parallelExecution in Test := false,
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true),
  assemblyMergeStrategy in assembly := {
    case PathList(ps @ _*) if ps.last endsWith ".txt"        => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".xml"        => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".class"      => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".xsd"        => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".dtd"        => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".dll"        => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("com.example.livy.WordCount")
  )



// Tasks
lazy val fj = taskKey[Unit]("")
fj := {
  val log = sLog.value

  val main   = "com.example.livy.WordCount"
  val fatJar = "./target/scala-2.11/root-assembly-1.0.0-SNAPSHOT.jar"
  val args = Seq()

  Process(
    Seq(
      "spark-submit",
      "--class",
      main,
      fatJar
    ) ++ args
  ) ! log
}

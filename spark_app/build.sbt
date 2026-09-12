ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.etl"
ThisBuild / version      := "0.1"

lazy val root = (project in file("."))
  .settings(
    name := "etl-job",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % "3.5.1" % Provided,
      "org.apache.spark" %% "spark-core" % "3.5.1" % Provided
    ),

    assembly / assemblyJarName := "etl-job-0.1.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "module-info.class"                  => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    }
  )
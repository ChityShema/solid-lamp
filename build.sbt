ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "ScalaETL_HR_DWH",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0",
      "org.apache.spark" %% "spark-sql" % "3.2.0",
      "org.postgresql" % "postgresql" % "42.3.1",
      "com.oracle.database.jdbc" % "ojdbc8" % "21.5.0.0"
    )
  )

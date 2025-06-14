@echo off
setlocal enabledelayedexpansion

:: Project name
set PROJECT_NAME=ScalaETL_HR_DWH

echo Creating Scala ETL Project Structure...

:: Create main directories
mkdir %PROJECT_NAME%
cd %PROJECT_NAME%
mkdir src\main\scala\com\hr\config
mkdir src\main\scala\com\hr\models
mkdir src\main\scala\com\hr\services
mkdir src\main\scala\com\hr\utils
mkdir src\main\resources
mkdir src\test\scala\com\hr\config
mkdir src\test\scala\com\hr\models
mkdir src\test\scala\com\hr\services
mkdir src\test\scala\com\hr\utils
mkdir src\test\resources
mkdir data\input
mkdir data\output
mkdir scripts
mkdir project

:: Create build.sbt
echo ThisBuild / version := "0.1.0-SNAPSHOT"> build.sbt
echo ThisBuild / scalaVersion := "2.12.15">> build.sbt
echo.>> build.sbt
echo lazy val root = (project in file("."))>> build.sbt
echo   .settings(>> build.sbt
echo     name := "ScalaETL_HR_DWH",>> build.sbt
echo     libraryDependencies ++= Seq(>> build.sbt
echo       "org.apache.spark" %%%% "spark-core" %% "3.2.0",>> build.sbt
echo       "org.apache.spark" %%%% "spark-sql" %% "3.2.0",>> build.sbt
echo       "org.postgresql" %% "postgresql" %% "42.3.1",>> build.sbt
echo       "com.oracle.database.jdbc" %% "ojdbc8" %% "21.5.0.0",>> build.sbt
echo       "com.typesafe" %% "config" %% "1.4.2",>> build.sbt
echo       "org.scalatest" %%%% "scalatest" %% "3.2.9" %% Test,>> build.sbt
echo       "org.slf4j" %% "slf4j-api" %% "1.7.32",>> build.sbt
echo       "ch.qos.logback" %% "logback-classic" %% "1.2.6">> build.sbt
echo     )>> build.sbt
echo   )>> build.sbt

:: Create application.conf
echo database {> src\main\resources\application.conf
echo   oracle {>> src\main\resources\application.conf
echo     url = ${?ORACLE_URL}>> src\main\resources\application.conf
echo     user = ${?ORACLE_USER}>> src\main\resources\application.conf
echo     password = ${?ORACLE_PASSWORD}>> src\main\resources\application.conf
echo   }>> src\main\resources\application.conf
echo   postgres {>> src\main\resources\application.conf
echo     url = ${?POSTGRES_URL}>> src\main\resources\application.conf
echo     user = ${?POSTGRES_USER}>> src\main\resources\application.conf
echo     password = ${?POSTGRES_PASSWORD}>> src\main\resources\application.conf
echo   }>> src\main\resources\application.conf
echo }>> src\main\resources\application.conf
echo.>> src\main\resources\application.conf
echo paths {>> src\main\resources\application.conf
echo   bonuses = "data/input/bonuses.csv">> src\main\resources\application.conf
echo   training = "data/input/training_scores.csv">> src\main\resources\application.conf
echo }>> src\main\resources\application.conf

:: Create SchemaUtils.scala
echo package com.hr.utils> src\main\scala\com\hr\utils\SchemaUtils.scala
echo.>> src\main\scala\com\hr\utils\SchemaUtils.scala
echo import org.apache.spark.sql.types._>> src\main\scala\com\hr\utils\SchemaUtils.scala
echo.>> src\main\scala\com\hr\utils\SchemaUtils.scala
echo object SchemaUtils {>> src\main\scala\com\hr\utils\SchemaUtils.scala
echo   val bonusesSchema = StructType(Array(>> src\main\scala\com\hr\utils\SchemaUtils.scala
echo     StructField("employee_id", IntegerType, false),>> src\main\scala\com\hr\utils\SchemaUtils.scala
echo     StructField("bonus_amount", DoubleType, false),>> src\main\scala\com\hr\utils\SchemaUtils.scala
echo     StructField("bonus_date", DateType, false),>> src\main\scala\com\hr\utils\SchemaUtils.scala
echo     StructField("performance_rating", StringType, false)>> src\main\scala\com\hr\utils\SchemaUtils.scala
echo   ))>> src\main\scala\com\hr\utils\SchemaUtils.scala
echo }>> src\main\scala\com\hr\utils\SchemaUtils.scala

:: Create TopPerformer.scala
echo package com.hr.models> src\main\scala\com\hr\models\TopPerformer.scala
echo.>> src\main\scala\com\hr\models\TopPerformer.scala
echo case class TopPerformer(>> src\main\scala\com\hr\models\TopPerformer.scala
echo   employee_id: Int,>> src\main\scala\com\hr\models\TopPerformer.scala
echo   first_name: String,>> src\main\scala\com\hr\models\TopPerformer.scala
echo   last_name: String,>> src\main\scala\com\hr\models\TopPerformer.scala
echo   department_id: Option[Int],>> src\main\scala\com\hr\models\TopPerformer.scala
echo   job_id: String,>> src\main\scala\com\hr\models\TopPerformer.scala
echo   bonus_amount: Double,>> src\main\scala\com\hr\models\TopPerformer.scala
echo   training_score: Double,>> src\main\scala\com\hr\models\TopPerformer.scala
echo   performance_rating: String,>> src\main\scala\com\hr\models\TopPerformer.scala
echo   composite_score: Double>> src\main\scala\com\hr\models\TopPerformer.scala
echo ) extends Serializable>> src\main\scala\com\hr\models\TopPerformer.scala

:: Create build.properties
echo sbt.version=1.6.2> project\build.properties

:: Create plugins.sbt
echo.> project\plugins.sbt

:: Create .gitignore
echo # Scala specific> .gitignore
echo *.class>> .gitignore
echo *.log>> .gitignore
echo target/>> .gitignore
echo project/target/>> .gitignore
echo project/project/>> .gitignore
echo.>> .gitignore
echo # IDE specific>> .gitignore
echo .idea/>> .gitignore
echo .bsp/>> .gitignore
echo .vscode/>> .gitignore
echo *.iml>> .gitignore
echo.>> .gitignore
echo # Data files>> .gitignore
echo data/output/*>> .gitignore
echo *.csv>> .gitignore
echo *.parquet>> .gitignore
echo.>> .gitignore
echo # Environment variables>> .gitignore
echo .env>> .gitignore

:: Create run script
echo spark-submit ^> scripts\run.sh
echo   --class com.hr.TopPerformersETL ^>> scripts\run.sh
echo   --master local[*] ^>> scripts\run.sh
echo   target/scala-2.12/scalaetl_hr_dwh_2.12-0.1.0-SNAPSHOT.jar>> scripts\run.sh

echo Project structure created successfully!
echo.
echo To get started:
echo 1. cd %PROJECT_NAME%
echo 2. sbt compile
echo.
pause

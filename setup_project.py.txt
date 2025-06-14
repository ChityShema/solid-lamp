import os
import stat

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")

def create_file(path, content=""):
    with open(path, 'w') as f:
        f.write(content)
    print(f"Created file: {path}")

def setup_project():
    # Project name
    PROJECT_NAME = "ScalaETL_HR_DWH"
    
    # Create main project directory
    create_directory(PROJECT_NAME)
    
    # Create directory structure
    directories = [
        "src/main/scala/com/hr/config",
        "src/main/scala/com/hr/models",
        "src/main/scala/com/hr/services",
        "src/main/scala/com/hr/utils",
        "src/main/resources",
        "src/test/scala/com/hr/config",
        "src/test/scala/com/hr/models",
        "src/test/scala/com/hr/services",
        "src/test/scala/com/hr/utils",
        "src/test/resources",
        "data/input",
        "data/output",
        "scripts",
        "project"
    ]
    
    for dir_path in directories:
        create_directory(os.path.join(PROJECT_NAME, dir_path))

    # Create build.sbt
    build_sbt_content = """ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "ScalaETL_HR_DWH",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0",
      "org.apache.spark" %% "spark-sql" % "3.2.0",
      "org.postgresql" % "postgresql" % "42.3.1",
      "com.oracle.database.jdbc" % "ojdbc8" % "21.5.0.0",
      "com.typesafe" % "config" % "1.4.2",
      "org.scalatest" %% "scalatest" % "3.2.9" % Test,
      "org.slf4j" % "slf4j-api" % "1.7.32",
      "ch.qos.logback" % "logback-classic" % "1.2.6"
    )
  )
"""
    
    # Create .gitignore
    gitignore_content = """# Scala specific
*.class
*.log
target/
project/target/
project/project/

# IDE specific
.idea/
.bsp/
.vscode/
*.iml

# SBT specific
.cache
.history
.lib/
dist/*
lib_managed/
src_managed/
project/boot/
project/plugins/project/

# Data files
data/output/*
*.csv
*.parquet

# Environment variables
.env
"""

    # Create application.conf
    app_conf_content = """database {
  oracle {
    url = ${?ORACLE_URL}
    user = ${?ORACLE_USER}
    password = ${?ORACLE_PASSWORD}
  }
  postgres {
    url = ${?POSTGRES_URL}
    user = ${?POSTGRES_USER}
    password = ${?POSTGRES_PASSWORD}
  }
}

paths {
  bonuses = "data/input/bonuses.csv"
  training = "data/input/training_scores.csv"
}
"""

    # Create run script
    run_script_content = """spark-submit \
  --class com.hr.TopPerformersETL \
  --master local[*] \
  target/scala-2.12/scalaetl_hr_dwh_2.12-0.1.0-SNAPSHOT.jar
"""

    # Create files
    files_to_create = {
        "build.sbt": build_sbt_content,
        ".gitignore": gitignore_content,
        "project/build.properties": "sbt.version=1.6.2",
        "project/plugins.sbt": "",
        "src/main/resources/application.conf": app_conf_content,
        "src/main/resources/log4j.properties": "log4j.rootLogger=INFO, console",
        "src/test/resources/test.conf": "",
        "scripts/run.sh": run_script_content,
        "README.md": "# Scala ETL HR DWH\n\nETL pipeline for processing HR data"
    }

    for file_path, content in files_to_create.items():
        create_file(os.path.join(PROJECT_NAME, file_path), content)

    # Make run script executable
    os.chmod(os.path.join(PROJECT_NAME, "scripts/run.sh"), 
             stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

    # Create Scala source files
    scala_files = {
        "src/main/scala/com/hr/models/TopPerformer.scala": """package com.hr.models

case class TopPerformer(
  employee_id: Int,
  first_name: String,
  last_name: String,
  department_id: Option[Int],
  job_id: String,
  bonus_amount: Double,
  training_score: Double,
  performance_rating: String,
  composite_score: Double
) extends Serializable
""",
        "src/main/scala/com/hr/services/DataReader.scala": """package com.hr.services

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.hr.utils.SchemaUtils

class DataReader(spark: SparkSession) {
  def readBonuses(path: String): DataFrame = {
    spark.read
      .schema(SchemaUtils.bonusesSchema)
      .option("header", "true")
      .option("mode", "FAILFAST")
      .csv(path)
  }
}
""",
        "src/main/scala/com/hr/utils/SchemaUtils.scala": """package com.hr.utils

import org.apache.spark.sql.types._

object SchemaUtils {
  val bonusesSchema = StructType(Array(
    StructField("employee_id", IntegerType, false),
    StructField("bonus_amount", DoubleType, false),
    StructField("bonus_date", DateType, false),
    StructField("performance_rating", StringType, false)
  ))
}
"""
    }

    for file_path, content in scala_files.items():
        create_file(os.path.join(PROJECT_NAME, file_path), content)

    print("\nProject structure created successfully!")
    print(f"\nTo get started:")
    print(f"1. cd {PROJECT_NAME}")
    print("2. sbt compile")

if __name__ == "__main__":
    setup_project()

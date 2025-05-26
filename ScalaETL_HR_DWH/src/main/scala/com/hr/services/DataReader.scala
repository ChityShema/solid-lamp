package com.hr.services

import com.hr.config.ConfigurationLoader
import com.hr.models._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataReader(spark: SparkSession) {
  import spark.implicits._

  def readBonuses(): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ConfigurationLoader.getInputPaths("bonuses"))
  }

  def readTrainingScores(): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ConfigurationLoader.getInputPaths("training"))
  }

  def readEmployees(): DataFrame = {
    val pgConfig = ConfigurationLoader.getDatabaseConfig("postgres")

    spark.read
      .format("jdbc")
      .option("url", pgConfig("url"))
      .option("dbtable", "employees")
      .option("user", pgConfig("user"))
      .option("password", pgConfig("password"))
      .option("driver", "org.postgresql.Driver")
      .load()
  }
}

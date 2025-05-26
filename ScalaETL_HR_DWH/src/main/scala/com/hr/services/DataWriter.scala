package com.hr.services

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.hr.config.ConfigurationLoader

class DataWriter {
  def writeToPostgres(df: DataFrame, tableName: String): Unit = {
    val pgConfig = ConfigurationLoader.getDatabaseConfig("postgres")

    df.write
      .format("jdbc")
      .option("url", pgConfig("url"))
      .option("dbtable", tableName)
      .option("user", pgConfig("user"))
      .option("password", pgConfig("password"))
      .option("driver", "org.postgresql.Driver")
      .mode(SaveMode.Overwrite)
      .save()
  }
}
// TopPerformersETL.scala
package com.hr

import org.apache.spark.sql.SparkSession
import com.hr.services.{DataReader, DataProcessor, DataWriter}

object TopPerformersETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HR Top Performers ETL")
      .master("local[*]")
      .getOrCreate()

    try {
      // Initialize services
      val reader = new DataReader(spark)
      val processor = new DataProcessor(spark)
      val writer = new DataWriter()

      // Read data
      val bonusesDF = reader.readBonuses()
      val trainingDF = reader.readTrainingScores()
      val employeesDF = reader.readEmployees()

      // Process data
      val topPerformersDF = processor.processTopPerformers(
        bonusesDF, trainingDF, employeesDF)

      // Write results
      writer.writeToPostgres(topPerformersDF, "topperformers")

      println("ETL process completed successfully!")
    } catch {
      case e: Exception =>
        println(s"ETL process failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}

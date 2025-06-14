// TopPerformersETL.scala
package com.hr

import org.apache.spark.sql.SparkSession
import com.hr.services.{DataProcessor, DataReader, DataWriter}
import org.slf4j.LoggerFactory

object TopPerformersETL {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // Set log4j configuration before creating SparkSession
    System.setProperty("log4j.configuration", "log4j.properties")
    val spark = SparkSession.builder()
      .appName("HR Top Performers ETL")
      .master("local[*]")
      .config("spark.log4j.configuration", "log4j.properties")
      .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j.properties")
      .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.properties")
      .getOrCreate()
    logger.info("SparkSession created with custom logging configuration")

    try {
      // Initialize services
      val reader = new DataReader(spark)
      val processor = new DataProcessor(spark)
      val writer = new DataWriter()

      // Read data
      val bonusesDF = reader.readBonuses()
      val trainingDF = reader.readTrainingScores()
      val employeesDF = reader.readEmployees()

      // Process top performers
      val topPerformersDF = processor.processTopPerformers(
        bonusesDF, trainingDF, employeesDF)

      // Calculate job switches
      val jobSwitchesDF = processor.calculateJobSwitches(employeesDF)

      // Calculate retention metrics
      val retentionMetricsDF = processor.calculateRetentionMetrics(
        employeesDF, bonusesDF, trainingDF)

      // Write results
      writer.writeToPostgres(topPerformersDF, "top_performers")
      writer.writeToPostgres(jobSwitchesDF, "job_switches")
      writer.writeToPostgres(retentionMetricsDF, "retention_metrics")

      logger.info("ETL process completed successfully!")
    } catch {
      case e: Exception =>
        logger.error(s"ETL process failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
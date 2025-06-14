package com.hr

import org.apache.spark.sql.SparkSession
import com.hr.services.{DataProcessor, DataReader}
import org.slf4j.LoggerFactory

object TopPerformers {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // Set log4j configuration before creating SparkSession
    System.setProperty("log4j.configuration", "log4j.properties")
    val spark = SparkSession.builder()
      .appName("HR Top Performers")
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

      // Read data
      val bonusesDF = reader.readBonuses()
      val trainingDF = reader.readTrainingScores()
      val employeesDF = reader.readEmployees()

      // Process top performers
      val topPerformersDF = processor.processTopPerformers(
        bonusesDF, trainingDF, employeesDF)

      // Display results
      println("\n=== Top Performers Analysis ===")
      topPerformersDF.show(truncate = false)
      
      logger.info("Top Performers analysis completed successfully!")
    } catch {
      case e: Exception =>
        logger.error(s"Analysis failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
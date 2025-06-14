package com.hr

import org.apache.spark.sql.SparkSession
import com.hr.services.{DataProcessor, DataReader, DataWriter}
import org.slf4j.LoggerFactory

object OverlappingJobsETL {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // Set log4j configuration before creating SparkSession
    System.setProperty("log4j.configuration", "log4j.properties")
    val spark = SparkSession.builder()
      .appName("HR Overlapping Jobs ETL")
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

      // Read employee data
      val employeesDF = reader.readEmployees()

      // Process overlapping job assignments
      val overlappingJobsDF = processor.analyzeOverlappingJobs(employeesDF)

      // Write results
      writer.writeToPostgres(overlappingJobsDF, "overlapping_jobs")

      logger.info("Overlapping Jobs ETL process completed successfully!")
    } catch {
      case e: Exception =>
        logger.error(s"ETL process failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
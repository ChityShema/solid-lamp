// CompensationAnalysisETL.scala
package com.hr

import org.apache.spark.sql.SparkSession
import com.hr.services.{DataProcessor, DataReader, DataWriter}
import org.slf4j.LoggerFactory

object CompensationAnalysisETL {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // Set log4j configuration before creating SparkSession
    System.setProperty("log4j.configuration", "log4j.properties")
    val spark = SparkSession.builder()
      .appName("HR Compensation Analysis ETL")
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

      // Read required data
      val employeesDF = reader.readEmployees()
      val departmentsDF = reader.readDepartments()
      val jobsDF = reader.readJobs()
      val locationsDF = reader.readLocations()
      val countriesDF = reader.readCountries()
      val regionsDF = reader.readRegions()

      // Process compensation analysis
      val compensationAnalysisDF = processor.processCompensationAnalysis(
        employeesDF,
        departmentsDF,
        jobsDF,
        locationsDF,
        countriesDF,
        regionsDF
      )

      // Write results
      writer.writeToPostgres(compensationAnalysisDF, "compensation_analysis")

      logger.info("Compensation Analysis ETL process completed successfully!")
    } catch {
      case e: Exception =>
        logger.error(s"ETL process failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
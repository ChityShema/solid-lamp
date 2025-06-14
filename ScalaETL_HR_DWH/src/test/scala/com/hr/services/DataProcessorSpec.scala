package com.hr.services

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class DataProcessorSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  var spark: SparkSession = _
  var dataProcessor: DataProcessor = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DataProcessorTest")
      .master("local[*]")
      .getOrCreate()
    dataProcessor = new DataProcessor(spark)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  describe("DataProcessor") {
    describe("processTopPerformers") {
      it("should calculate correct composite scores and aggregations") {
        // Create test data
        val employeesData = Seq(
          (1, "John", "Doe", 101, "JOB1"),
          (2, "Jane", "Smith", 102, "JOB2")
        )
        val employeesDF = spark.createDataFrame(employeesData)
          .toDF("employee_id", "first_name", "last_name", "department_id", "job_id")

        val bonusesData = Seq(
          (1, 1000.0, "Excellent"),
          (1, 2000.0, "Good"),
          (2, 1500.0, "Average")
        )
        val bonusesDF = spark.createDataFrame(bonusesData)
          .toDF("employee_id", "bonus_amount", "performance_rating")

        val trainingData = Seq(
          (1, 95.0, "Completed"),
          (1, 85.0, "Completed"),
          (2, 90.0, "Completed"),
          (2, 80.0, "In Progress") // Should be filtered out
        )
        val trainingDF = spark.createDataFrame(trainingData)
          .toDF("employee_id", "training_score", "completion_status")

        // Process data
        val result = dataProcessor.processTopPerformers(bonusesDF, trainingDF, employeesDF)

        // Verify results
        result.cache()

        // Check schema
        result.columns should contain allOf(
          "employee_id", "first_name", "last_name", "department_id", "job_id",
          "total_bonus", "avg_training_score", "performance_score", "composite_score"
        )

        // Check specific values for employee 1
        val emp1 = result.where("employee_id = 1").collect()(0)
        emp1.getAs[Double]("total_bonus") shouldBe 3000.0
        emp1.getAs[Double]("performance_score") shouldBe 4.5 // Average of 5.0 and 4.0
        emp1.getAs[Double]("avg_training_score") shouldBe 90.0 // Average of 95 and 85

        // Verify composite score calculation (performance_score * 0.4 + avg_training_score * 0.6)
        val expectedCompositeScore1 = 4.5 * 0.4 + 90.0 * 0.6
        emp1.getAs[Double]("composite_score") shouldBe expectedCompositeScore1 +- 0.01

        // Check specific values for employee 2
        val emp2 = result.where("employee_id = 2").collect()(0)
        emp2.getAs[Double]("total_bonus") shouldBe 1500.0
        emp2.getAs[Double]("performance_score") shouldBe 3.0
        emp2.getAs[Double]("avg_training_score") shouldBe 90.0
      }

      it("should handle empty dataframes") {
        val emptyDF = spark.emptyDataFrame
        val schema = spark.createDataFrame(Seq(
          (1, "John", "Doe", 101, "JOB1")
        )).toDF("employee_id", "first_name", "last_name", "department_id", "job_id")

        val result = dataProcessor.processTopPerformers(emptyDF, emptyDF, schema)
        result.count() shouldBe 0
      }

      it("should handle null values in performance ratings") {
        val employeesData = Seq((1, "John", "Doe", 101, "JOB1"))
        val employeesDF = spark.createDataFrame(employeesData)
          .toDF("employee_id", "first_name", "last_name", "department_id", "job_id")

        val bonusesData = Seq((1, 1000.0, null))
        val bonusesDF = spark.createDataFrame(bonusesData)
          .toDF("employee_id", "bonus_amount", "performance_rating")

        val trainingData = Seq((1, 90.0, "Completed"))
        val trainingDF = spark.createDataFrame(trainingData)
          .toDF("employee_id", "training_score", "completion_status")

        val result = dataProcessor.processTopPerformers(bonusesDF, trainingDF, employeesDF)

        val row = result.collect()(0)
        row.getAs[Double]("performance_score") shouldBe 2.0 // default value
      }
    }
  }

  describe("processCompensationAnalysis") {
    it("should calculate total compensation and salary rankings correctly") {
      // Create test data
      val employeesData = Seq(
        // employee_id, first_name, last_name, email, department_id, job_id, salary, commission_pct
        (1, "John", "Doe", "jdoe@example.com", 10, "JOB1", 50000.0, Some(0.1)),
        (2, "Jane", "Smith", "jsmith@example.com", 10, "JOB2", 60000.0, None),
        (3, "Bob", "Brown", "bbrown@example.com", 20, "JOB1", 45000.0, Some(0.15))
      )
      val employeesDF = spark.createDataFrame(employeesData)
        .toDF("employee_id", "first_name", "last_name", "email", "department_id", "job_id", "salary", "commission_pct")

      val departmentsData = Seq(
        // department_id, department_name, location_id
        (10, "Sales", 100),
        (20, "Marketing", 200)
      )
      val departmentsDF = spark.createDataFrame(departmentsData)
        .toDF("department_id", "department_name", "location_id")

      val jobsData = Seq(
        // job_id, job_title
        ("JOB1", "Sales Representative"),
        ("JOB2", "Sales Manager")
      )
      val jobsDF = spark.createDataFrame(jobsData)
        .toDF("job_id", "job_title")

      val locationsData = Seq(
        // location_id, country_id
        (100, "US"),
        (200, "UK")
      )
      val locationsDF = spark.createDataFrame(locationsData)
        .toDF("location_id", "country_id")

      val countriesData = Seq(
        // country_id, region_id
        ("US", 1),
        ("UK", 2)
      )
      val countriesDF = spark.createDataFrame(countriesData)
        .toDF("country_id", "region_id")

      val regionsData = Seq(
        // region_id, region_name
        (1, "Americas"),
        (2, "Europe")
      )
      val regionsDF = spark.createDataFrame(regionsData)
        .toDF("region_id", "region_name")

      // Process data
      val result = dataProcessor.processCompensationAnalysis(
        employeesDF, departmentsDF, jobsDF, locationsDF, countriesDF, regionsDF)

      // Verify results
      result.cache()

      // Check schema
      result.columns should contain allOf(
        "employee_id", "first_name", "last_name", "email",
        "department_name", "job_title", "region_name",
        "total_comp", "salary_rank"
      )

      // Check specific values
      val resultRows = result.collect()

      // Check Jane Smith's record (highest salary in Sales dept)
      val janeRow = result.where("email = 'jsmith@example.com'").collect()(0)
      janeRow.getAs[String]("department_name") shouldBe "Sales"
      janeRow.getAs[String]("job_title") shouldBe "Sales Manager"
      janeRow.getAs[Double]("total_comp") shouldBe 60000.0 // No commission
      janeRow.getAs[Long]("salary_rank") shouldBe 1 // Highest in Sales dept

      // Check John Doe's record (commission included)
      val johnRow = result.where("email = 'jdoe@example.com'").collect()(0)
      johnRow.getAs[Double]("total_comp") shouldBe 55000.0 // 50000 + (50000 * 0.1)
      johnRow.getAs[Long]("salary_rank") shouldBe 2 // Second highest in Sales dept

      // Check Bob Brown's record (different department)
      val bobRow = result.where("email = 'bbrown@example.com'").collect()(0)
      bobRow.getAs[String]("department_name") shouldBe "Marketing"
      bobRow.getAs[String]("region_name") shouldBe "Europe"
      bobRow.getAs[Double]("total_comp") shouldBe 51750.0 // 45000 + (45000 * 0.15)
      bobRow.getAs[Long]("salary_rank") shouldBe 1 // Highest (only one) in Marketing dept
    }

    it("should handle null values and missing data") {
      // Create minimal test data with nulls
      val employeesData = Seq(
        (1, "John", "Doe", "jdoe@example.com", null, "JOB1", 50000.0, null)
      )
      val employeesDF = spark.createDataFrame(employeesData)
        .toDF("employee_id", "first_name", "last_name", "email", "department_id", "job_id", "salary", "commission_pct")

      val emptyDF = spark.emptyDataFrame

      // Create minimal required schema for empty DataFrames
      val departmentsDF = spark.createDataFrame(Seq((1, "Test", 1)))
        .toDF("department_id", "department_name", "location_id")
      val jobsDF = spark.createDataFrame(Seq(("JOB1", "Test")))
        .toDF("job_id", "job_title")
      val locationsDF = spark.createDataFrame(Seq((1, "US")))
        .toDF("location_id", "country_id")
      val countriesDF = spark.createDataFrame(Seq(("US", 1)))
        .toDF("country_id", "region_id")
      val regionsDF = spark.createDataFrame(Seq((1, "Test")))
        .toDF("region_id", "region_name")

      val result = dataProcessor.processCompensationAnalysis(
        employeesDF, departmentsDF, jobsDF, locationsDF, countriesDF, regionsDF)

      // Verify results
      result.cache()
      
      val row = result.collect()(0)
      row.getAs[Double]("total_comp") shouldBe 50000.0 // No commission added
      row.getAs[String]("department_name") shouldBe null // Left join preserves null
      row.getAs[Long]("salary_rank") shouldBe 1 // Only employee in null department
    }
  }

  describe("analyzeOverlappingJobs") {
    it("should identify overlapping job assignments correctly") {
      // Create test data with overlapping assignments
      val employeesData = Seq(
        // employee_id, first_name, last_name, job_id, start_date, end_date
        (1, "John", "Doe", "JOB1", "2023-01-01", "2023-06-30"),
        (1, "John", "Doe", "JOB2", "2023-05-01", "2023-12-31"), // Overlaps with JOB1
        (2, "Jane", "Smith", "JOB3", "2023-01-01", "2023-12-31"),
        (2, "Jane", "Smith", "JOB4", "2023-07-01", "2023-12-31"), // Overlaps with JOB3
        (3, "Bob", "Brown", "JOB5", "2023-01-01", "2023-06-30"),
        (3, "Bob", "Brown", "JOB6", "2023-07-01", "2023-12-31") // No overlap
      )
      val employeesDF = spark.createDataFrame(employeesData)
        .toDF("employee_id", "first_name", "last_name", "job_id", "start_date", "end_date")

      // Process data
      val result = dataProcessor.analyzeOverlappingJobs(employeesDF)

      // Verify results
      result.cache()

      // Check schema
      result.columns should contain allOf(
        "employee_id", "first_name", "last_name",
        "total_overlaps", "total_overlap_days", "overlap_details"
      )

      // Check specific values
      val resultRows = result.collect()

      // Should only include employees with overlapping assignments
      result.count() shouldBe 2 // Only John and Jane have overlaps

      // Check John's overlaps
      val johnRow = result.where("first_name = 'John'").collect()(0)
      johnRow.getAs[Long]("total_overlaps") shouldBe 1
      johnRow.getAs[Long]("total_overlap_days") shouldBe 61 // May and June overlap

      // Check Jane's overlaps
      val janeRow = result.where("first_name = 'Jane'").collect()(0)
      janeRow.getAs[Long]("total_overlaps") shouldBe 1
      janeRow.getAs[Long]("total_overlap_days") shouldBe 184 // July through December overlap

      // Bob should not be in results (no overlaps)
      result.where("first_name = 'Bob'").count() shouldBe 0
    }

    it("should handle null dates and empty data") {
      // Create test data with null end dates (current date should be used)
      val employeesData = Seq(
        (1, "John", "Doe", "JOB1", "2023-01-01", null),
        (1, "John", "Doe", "JOB2", "2023-06-01", null) // Should overlap
      )
      val employeesDF = spark.createDataFrame(employeesData)
        .toDF("employee_id", "first_name", "last_name", "job_id", "start_date", "end_date")

      // Process data
      val result = dataProcessor.analyzeOverlappingJobs(employeesDF)

      // Verify results
      result.cache()

      result.count() shouldBe 1 // Should find the overlap
      val row = result.collect()(0)
      row.getAs[Long]("total_overlaps") shouldBe 1

      // Test with empty DataFrame
      val emptyDF = spark.emptyDataFrame
      val emptyResult = dataProcessor.analyzeOverlappingJobs(emptyDF)
      emptyResult.count() shouldBe 0
    }
  }
}

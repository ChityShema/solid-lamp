package com.hr.services

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class DataProcessor(spark: SparkSession) {
  
  /**
   * Process compensation analysis by joining employee data with related tables
   * and calculating total compensation and salary rankings within departments.
   *
   * @param employeesDF DataFrame containing employee data
   * @param departmentsDF DataFrame containing department data
   * @param jobsDF DataFrame containing job data
   * @param locationsDF DataFrame containing location data
   * @param countriesDF DataFrame containing country data
   * @param regionsDF DataFrame containing region data
   * @return DataFrame with compensation analysis results
   */
  def processCompensationAnalysis(
    employeesDF: DataFrame,
    departmentsDF: DataFrame,
    jobsDF: DataFrame,
    locationsDF: DataFrame,
    countriesDF: DataFrame,
    regionsDF: DataFrame
  ): DataFrame = {
    import spark.implicits._
    
    // Calculate total compensation including commission
    val employeesWithTotalComp = employeesDF
      .withColumn("total_comp",
        col("salary") + coalesce(col("commission_pct") * col("salary"), lit(0)))

    // Create window spec for salary ranking within departments
    val windowSpec = Window
      .partitionBy("department_id")
      .orderBy(col("salary").desc)

    // Join all tables and calculate salary rank
    employeesWithTotalComp
      .join(departmentsDF, Seq("department_id"), "left")
      .join(jobsDF, Seq("job_id"), "left")
      .join(locationsDF, departmentsDF("location_id") === locationsDF("location_id"), "left")
      .join(countriesDF, locationsDF("country_id") === countriesDF("country_id"), "left")
      .join(regionsDF, countriesDF("region_id") === regionsDF("region_id"), "left")
      .withColumn("salary_rank", rank().over(windowSpec))
      .select(
        col("employee_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("department_name"),
        col("job_title"),
        col("region_name"),
        col("total_comp"),
        col("salary_rank")
      )
  }
  def processTopPerformers(
                            bonusesDF: DataFrame,
                            trainingDF: DataFrame,
                            employeesDF: DataFrame
                          ): DataFrame = {

    // Aggregate bonus data
    val bonusAgg = bonusesDF
      .groupBy("employee_id")
      .agg(
        sum("bonus_amount").as("total_bonus"),
        avg(when(col("performance_rating") === "Excellent", 5.0)
          .when(col("performance_rating") === "Good", 4.0)
          .when(col("performance_rating") === "Average", 3.0)
          .otherwise(2.0)).as("performance_score")
      )

    // Aggregate training scores
    val trainingAgg = trainingDF
      .where(col("completion_status") === "Completed")
      .groupBy("employee_id")
      .agg(avg("training_score").as("avg_training_score"))

    // Join and calculate final scores
    employeesDF
      .join(bonusAgg, "employee_id")
      .join(trainingAgg, "employee_id")
      .withColumn("composite_score",
        (col("performance_score") * 0.4) + (col("avg_training_score") * 0.6))
      .select(
        col("employee_id"),
        col("first_name"),
        col("last_name"),
        col("department_id"),
        col("job_id"),
        col("total_bonus"),
        col("avg_training_score"),
        col("performance_score"),
        col("composite_score")
      )
  }

  def calculateJobSwitches(employeesDF: DataFrame): DataFrame = {
    // Create window spec for ordering by employee and date
    val windowSpec = Window
      .partitionBy("employee_id")
      .orderBy("hire_date")
    
    // Calculate job switches by comparing current job with previous job
    employeesDF
      .withColumn("prev_job_id", lag("job_id", 1).over(windowSpec))
      .withColumn("is_job_switch", when(col("prev_job_id").isNotNull && 
        col("job_id") =!= col("prev_job_id"), 1).otherwise(0))
      .groupBy("employee_id")
      .agg(
        sum("is_job_switch").as("job_switches"),
        collect_list(
          when(col("is_job_switch") === 1,
            struct(
              col("prev_job_id").as("old_job_id"),
              col("job_id").as("new_job_id"),
              col("hire_date").as("switch_date")
            )
          )
        ).as("switch_history")
      )
  }

  def calculateRetentionMetrics(
    employeesDF: DataFrame,
    bonusesDF: DataFrame,
    trainingDF: DataFrame
  ): DataFrame = {
    
    // Calculate tenure in months
    val currentDate = LocalDate.now()
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    
    val employeesWithTenure = employeesDF
      .withColumn("tenure_months",
        months_between(current_date(), to_date(col("hire_date"))))

    // Get job switches
    val jobSwitchesDF = calculateJobSwitches(employeesDF)

    // Get performance metrics
    val performanceDF = processTopPerformers(bonusesDF, trainingDF, employeesDF)

    // Calculate retention score
    // Formula: (0.3 * normalized_performance) + (0.3 * normalized_training) + 
    // (0.2 * normalized_tenure) + (0.2 * (1 - normalized_job_switches))
    val retentionDF = employeesWithTenure
      .join(jobSwitchesDF, "employee_id")
      .join(performanceDF, "employee_id")
      .withColumn("normalized_performance", 
        col("performance_score") / lit(5.0))
      .withColumn("normalized_training",
        col("avg_training_score") / lit(100.0))
      .withColumn("normalized_tenure",
        col("tenure_months") / max("tenure_months").over())
      .withColumn("normalized_job_switches",
        col("job_switches") / max("job_switches").over())
      .withColumn("retention_score",
        (col("normalized_performance") * 0.3) +
        (col("normalized_training") * 0.3) +
        (col("normalized_tenure") * 0.2) +
        ((lit(1.0) - col("normalized_job_switches")) * 0.2))
      .select(
        col("employee_id"),
        col("avg_training_score"),
        col("performance_score"),
        col("job_switches"),
        col("tenure_months"),
        round(col("retention_score"), 2).as("retention_score")
      )

    retentionDF
  }

  /**
   * Analyze overlapping job assignments by identifying employees who have multiple
   * active job assignments in the same time period.
   *
   * @param employeesDF DataFrame containing employee job history data
   * @return DataFrame with overlapping job assignments analysis
   */
  def analyzeOverlappingJobs(employeesDF: DataFrame): DataFrame = {
    import spark.implicits._

    // Create window spec for finding overlapping assignments
    val windowSpec = Window
      .partitionBy("employee_id")
      .orderBy("start_date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // Find overlapping assignments
    val overlappingJobsDF = employeesDF
      .withColumn("end_date", 
        coalesce(col("end_date"), current_date()))
      .withColumn("prev_end_date", 
        lag("end_date", 1).over(Window.partitionBy("employee_id").orderBy("start_date")))
      .withColumn("is_overlapping",
        when(col("prev_end_date").isNotNull &&
          col("start_date").lt(col("prev_end_date")), true)
          .otherwise(false))
      .filter(col("is_overlapping") === true)
      .withColumn("overlap_duration_days",
        datediff(
          least(col("end_date"), col("prev_end_date")),
          col("start_date")))
      .select(
        col("employee_id"),
        col("first_name"),
        col("last_name"),
        col("job_id").as("current_job_id"),
        lag("job_id", 1).over(Window.partitionBy("employee_id").orderBy("start_date"))
          .as("overlapping_job_id"),
        col("start_date"),
        col("end_date"),
        col("overlap_duration_days")
      )
      .groupBy("employee_id", "first_name", "last_name")
      .agg(
        count("*").as("total_overlaps"),
        sum("overlap_duration_days").as("total_overlap_days"),
        collect_list(
          struct(
            col("current_job_id"),
            col("overlapping_job_id"),
            col("start_date"),
            col("end_date"),
            col("overlap_duration_days")
          )
        ).as("overlap_details")
      )
      .orderBy(col("total_overlaps").desc)

    overlappingJobsDF
  }
}
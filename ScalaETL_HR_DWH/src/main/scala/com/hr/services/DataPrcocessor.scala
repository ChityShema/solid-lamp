package com.hr.services

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataProcessor(spark: SparkSession) {
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
}

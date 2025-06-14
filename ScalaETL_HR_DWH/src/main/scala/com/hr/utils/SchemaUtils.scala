package com.hr.utils

import org.apache.spark.sql.types._

object SchemaUtils {
  val bonusesSchema = StructType(Array(
    StructField("employee_id", IntegerType, false),
    StructField("bonus_amount", DoubleType, false),
    StructField("bonus_date", DateType, false),
    StructField("performance_rating", StringType, false)
  ))
}

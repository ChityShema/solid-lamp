// Models.scala
package com.hr.models

case class Bonus(
                  employee_id: Int,
                  bonus_amount: Double,
                  bonus_date: String,
                  performance_rating: String
                ) extends Serializable

case class TrainingScore(
                          employee_id: Int,
                          training_score: Double,
                          training_date: String,
                          course_name: String,
                          completion_status: String
                        ) extends Serializable

case class Employee(
                     employee_id: Int,
                     first_name: String,
                     last_name: String,
                     email: String,
                     phone_number: String,
                     hire_date: String,
                     job_id: String,
                     salary: Double,
                     commission_pct: Option[Double],
                     manager_id: Option[Int],
                     department_id: Option[Int]
                   ) extends Serializable

case class JobSwitch(
                     employee_id: Int,
                     old_job_id: String,
                     new_job_id: String,
                     switch_date: String
                   ) extends Serializable

case class RetentionMetric(
                           employee_id: Int,
                           avg_training_score: Double,
                           performance_score: Double,
                           job_switches: Int,
                           tenure_months: Int,
                           retention_score: Double
                         ) extends Serializable
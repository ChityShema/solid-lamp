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

// ConfigurationLoader.scala
package com.hr.config

import com.typesafe.config.{Config, ConfigFactory}

object ConfigurationLoader {
  private val config: Config = ConfigFactory.load()

  def getDatabaseConfig(dbType: String): Map[String, String] = {
    val dbConfig = config.getConfig(s"database.$dbType")
    Map(
      "url" -> dbConfig.getString("url"),
      "user" -> dbConfig.getString("user"),
      "password" -> dbConfig.getString("password")
    )
  }

  def getInputPaths: Map[String, String] = {
    val pathsConfig = config.getConfig("paths")
    Map(
      "bonuses" -> pathsConfig.getString("bonuses"),
      "training" -> pathsConfig.getString("training")
    )
  }
}

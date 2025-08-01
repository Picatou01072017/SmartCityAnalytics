package com.ecommerce.analytics

import org.apache.spark.sql.{Dataset, SparkSession}
import com.ecommerce.models._
import com.typesafe.config.ConfigFactory

object DataIngestion {
  private val config = ConfigFactory.load()
  private val dataConfig = config.getConfig("app.data.input")

  def readTransactions(spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._
    val path = dataConfig.getString("transactions")
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .as[Transaction]
  }

  def readUsers(spark: SparkSession): Dataset[User] = {
    import spark.implicits._
    val path = dataConfig.getString("users")
    spark.read
      .option("multiline", true)
      .json(path)
      .as[User]
  }

  def readProducts(spark: SparkSession): Dataset[Product] = {
    import spark.implicits._
    val path = dataConfig.getString("products")
    spark.read.parquet(path).as[Product]
  }

  def readMerchants(spark: SparkSession): Dataset[Merchant] = {
    import spark.implicits._
    val path = dataConfig.getString("merchants")
    spark.read
      .option("header", "true")
      .csv(path)
      .as[Merchant]
  }
}
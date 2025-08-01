package com.ecommerce.analytics

import org.apache.spark.sql.{Dataset, SparkSession}
import com.ecommerce.models._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.DayOfWeek

import scala.Double

object DataIngestion {
  private val config = ConfigFactory.load()
  private val dataConfig = config.getConfig("app.data.input")

  private def report[T](name: String, total: Long, valid: Long): Unit = {
    println(s"[$name] Lignes lues : $total | Lignes valides : $valid")
  }

  def readTransactions(spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._
    try {
      val path = dataConfig.getString("transactions")
      val raw = spark.read.option("header", "true").option("inferSchema", "true").csv(path).as[Transaction]
      val total = raw.count()
      val valid = validateTransactions(raw)
      report("Transactions", total, valid.count())
      valid
    } catch {
      case e: Exception =>
        println(s"Erreur lors de la lecture des transactions: ${e.getMessage}")
        spark.emptyDataset[Transaction]
    }
  }

  def readUsers(spark: SparkSession): Dataset[User] = {
    import spark.implicits._
    try {
      val path = dataConfig.getString("users")
      val raw = spark.read.option("multiline", true).json(path).as[User]
      val total = raw.count()
      val valid = validateUsers(raw)
      report("Users", total, valid.count())
      valid
    } catch {
      case e: Exception =>
        println(s"Erreur lors de la lecture des utilisateurs: ${e.getMessage}")
        spark.emptyDataset[User]
    }
  }

  def readProducts(spark: SparkSession): Dataset[Product] = {
    import spark.implicits._
    try {
      val path = dataConfig.getString("products")
      val raw = spark.read.parquet(path).as[Product]
      val total = raw.count()
      val valid = validateProducts(raw)
      report("Products", total, valid.count())
      valid
    } catch {
      case e: Exception =>
        println(s"Erreur lors de la lecture des produits: ${e.getMessage}")
        spark.emptyDataset[Product]
    }
  }

  def readMerchants(spark: SparkSession): Dataset[Merchant] = {
    import spark.implicits._
    try {
      val path = dataConfig.getString("merchants")
      val raw = spark.read.option("header", "true").csv(path).as[Merchant]
      val total = raw.count()
      val valid = validateMerchants(raw)
      report("Merchants", total, valid.count())
      valid
    } catch {
      case e: Exception =>
        println(s"Erreur lors de la lecture des marchands: ${e.getMessage}")
        spark.emptyDataset[Merchant]
    }
  }


  // Fonctions de validation

  def validateTransactions(ds: Dataset[Transaction]): Dataset[Transaction] = {
    ds.filter(t => t.amount > .0 && t.timestamp.length == 14)
  }

  def validateUsers(ds: Dataset[User]): Dataset[User] = {
    ds.filter(u => u.age >= 16 && u.age <= 100 && u.annual_income > 0)
  }

  def validateProducts(ds: Dataset[Product]): Dataset[Product] = {
    ds.filter(p => p.price > 0 && p.rating >= 1.0 && p.rating <= 5.0)
  }

  def validateMerchants(ds: Dataset[Merchant]): Dataset[Merchant] = {
    ds.filter(m => m.commission_rate.toDouble >= 0.0 && m.commission_rate.toDouble <= 1.0)
  }
}
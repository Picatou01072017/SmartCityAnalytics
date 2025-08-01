package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession

object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EcommerceAnalytics")
      .master("local[*]")
      .getOrCreate()
  println("Hello")
    // appel aux modules
//    val transactions = DataIngestion.readTransactions(spark)

//    transactions.show()
    // enriched.write.mode("overwrite").csv("output/result.csv")
  }
}


package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Analytics {

  def merchantKPIReport(enriched: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val baseAgg = enriched.groupBy(
      col("merchant_id"),
      col("name"),
//      col("category").as("merchant_category"),
      col("region")
    )
    .agg(
      sum("amount").as("total_revenue"),
      count("transaction_id").as("total_transactions"),
      countDistinct("user_id").as("unique_users"),
      avg("amount").as("avg_transaction_amount"),
      sum(expr("amount * commission_rate")).as("total_commission")
    )

    val windowSpec = Window.partitionBy("region").orderBy(col("total_revenue").desc)


    val ranked = baseAgg.withColumn("rank_in_category_region", rank().over(windowSpec))

  val ageAgg = enriched.groupBy("merchant_id", "age_group").count()
    .groupBy("merchant_id")
    .pivot("age_group", Seq("Jeune", "Adulte", "Age Moyen", "Senior"))
    .sum("count")
    .na.fill(0)
    .withColumnRenamed("Jeune", "age_jeune")
    .withColumnRenamed("Adulte", "age_adulte")
    .withColumnRenamed("Age Moyen", "age_moyen")
    .withColumnRenamed("Senior", "age_senior")

  val finalReport = ranked.join(ageAgg, Seq("merchant_id"), "left")

  finalReport
}

  def userCohortAnalysis(enriched: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // Extraire date de transaction et premier mois d'achat par utilisateur
    val txWithDate = enriched.withColumn("transaction_month", date_format(to_date(col("timestamp"), "yyyyMMddHHmmss"), "yyyy-MM"))

    val userFirstMonth = txWithDate.groupBy("user_id")
      .agg(min("transaction_month"))

    val cohortData = txWithDate
      .join(userFirstMonth, "user_id")
      .groupBy("transaction_month")
      .agg(countDistinct("user_id").as("retained_users"))
      .orderBy("transaction_month")

    cohortData
  }
}


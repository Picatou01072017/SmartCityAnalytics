package com.ecommerce.analytics


import com.ecommerce.models._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.DayOfWeek

object DataTransformation {

  // UDF pour extraire les caractéristiques temporelles
  val extractTimeFeatures = udf((ts: String) => {
    try {
      val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
      val dt = LocalDateTime.parse(ts, formatter)
      val hour = dt.getHour
      val dayOfWeek = dt.getDayOfWeek.toString
      val month = dt.getMonth.toString
      val isWeekend = if (dt.getDayOfWeek == DayOfWeek.SATURDAY || dt.getDayOfWeek == DayOfWeek.SUNDAY) 1 else 0

      val dayPeriod = hour match {
        case h if h >= 6 && h < 12  => "Morning"
        case h if h >= 12 && h < 18 => "Afternoon"
        case h if h >= 18 && h < 22 => "Evening"
        case _ => "Night"
      }

      val isWorkingHour = if (hour >= 9 && hour < 17) 1 else 0

      TimeFeatures(hour, dayOfWeek, month, isWeekend, dayPeriod, isWorkingHour)
    } catch {
      case _: Exception => TimeFeatures(0, "UNKNOWN", "UNKNOWN", 0, "UNKNOWN", 0)
    }
  })

  def enrichTransactionData(
                             transactions: Dataset[Transaction],
                             users: Dataset[User],
                             merchants: Dataset[Merchant],
//                             products: Dataset[Product],
                           )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val joined = transactions
      .join(users, "user_id")
//      .join(products, "product_id")
      .join(broadcast(merchants), "merchant_id")

    val enriched = joined
      .withColumn("time_features", extractTimeFeatures($"timestamp"))
      .withColumn("hour", col("time_features.hour"))
      .withColumn("day_of_week", col("time_features.dayOfWeek"))
      .withColumn("month", col("time_features.month"))
      .withColumn("is_weekend", col("time_features.isWeekend"))
      .withColumn("day_period", col("time_features.dayPeriod"))
      .withColumn("is_working_hour", col("time_features.isWorkingHour"))
      .drop("time_features")

    val windowByUser = Window.partitionBy("user_id").orderBy("timestamp")
    val windowCount = Window.partitionBy("user_id")

    enriched
      .withColumn("transaction_rank", row_number().over(windowByUser))
      .withColumn("transaction_count", count("transaction_id").over(windowCount))
      .withColumn("age_group", when($"age" < 25, "Jeune")
        .when($"age" >= 25 && $"age" <= 44, "Adulte")
        .when($"age" >= 45 && $"age" <= 64, "Age Moyen")
        .otherwise("Senior"))
  }

  // TODO: calculer le montant cumule au lieu du nombre totall de transaction
  def computeUserActivity(transactions: Dataset[Transaction])(implicit spark: SparkSession): DataFrame = {

    val txWithDate = transactions.withColumn("date", to_date(col("timestamp"), "yyyyMMddHHmmss"))

    // Générer toutes les paires (user_id, date) distinctes
    val userDate = txWithDate.select("user_id", "date").distinct()

    // Créer une fenêtre de 7 jours glissants
    val window7d = Window.partitionBy("user_id").orderBy(col("date").cast("timestamp").cast("long")).rangeBetween(-6 * 86400, 0)

    val withActivity = userDate
      .withColumn("active_days_7d", count("date").over(window7d))
      .withColumn("is_active_user_7d", when(col("active_days_7d") >= 5, 1).otherwise(0))

    withActivity
  }

}
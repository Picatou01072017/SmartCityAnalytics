// Analytics.scala
package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window

object Analytics {

  // ------------------------------------------------------------------------------------
  // 1) Rapport KPI par Zone (équivalent "merchantKPIReport", mais pour les zones)
  //    - agrégats trafic (véhicules, vitesse, densité)
  //    - météo moyenne
  //    - taux d’incidents, distribution des niveaux de congestion
  //    - ranking par district (ex-"region")
  // ------------------------------------------------------------------------------------
  def zoneKPIReport(enriched: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val baseAgg = enriched
      .groupBy(
        $"zone_id", $"zone_name", $"district"
      )
      .agg(
        F.countDistinct($"event_id").as("events"),
        F.sum($"vehicle_count").as("vehicle_count_sum"),
        F.avg($"vehicle_count").as("vehicle_count_avg"),
        F.avg($"avg_speed").as("avg_speed_avg"),
        F.avg($"traffic_density").as("traffic_density_avg"),
        F.sum(F.when($"is_incident" === 1, 1).otherwise(0)).as("incidents"),
        // météo
        F.avg($"temperature").as("temperature_avg"),
        F.avg($"humidity").as("humidity_avg"),
        F.avg($"pressure").as("pressure_avg"),
        F.avg($"wind_speed").as("wind_speed_avg"),
        F.avg($"rainfall").as("rainfall_avg")
      )
      .withColumn("incident_rate", F.col("incidents") / F.col("events"))

    // Distribution des niveaux de congestion (Low/Moderate/High/Severe) en colonnes
    val congestionPivot = enriched
      .groupBy($"zone_id")
      .pivot($"congestion_level", Seq("Low", "Moderate", "High", "Severe"))
      .agg(F.count("*"))
      .na.fill(0)
      .withColumnRenamed("Low", "congestion_low")
      .withColumnRenamed("Moderate", "congestion_moderate")
      .withColumnRenamed("High", "congestion_high")
      .withColumnRenamed("Severe", "congestion_severe")

    val joined = baseAgg.join(congestionPivot, Seq("zone_id"), "left")

    // Ranking par district sur le volume de trafic
    val w = Window.partitionBy($"district").orderBy(F.col("vehicle_count_sum").desc_nulls_last)
    val ranked = joined.withColumn("rank_in_district", F.rank().over(w))

    ranked
  }

  // ------------------------------------------------------------------------------------
  // 2) Rapport KPI par Capteur
  //    - agrégats trafic
  //    - incidents
  //    - "uptime" observé = nb jours avec événements / nb jours calendrier sur la période
  // ------------------------------------------------------------------------------------
  def sensorKPIReport(enriched: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // bornes de la période globale
    val bounds = enriched
      .agg(F.min($"event_ts").as("min_ts"), F.max($"event_ts").as("max_ts"))
      .collect()(0)

    val minTs = bounds.getAs[java.sql.Timestamp]("min_ts")
    val maxTs = bounds.getAs[java.sql.Timestamp]("max_ts")
    val totalDays = math.max(1L,
      java.time.Duration.between(minTs.toInstant, maxTs.toInstant).toDays + 1
    )

    val bySensorAgg = enriched
      .groupBy($"sensor_id", $"sensor_type", $"zone_id", $"zone_name", $"district", $"status")
      .agg(
        F.countDistinct($"event_id").as("events"),
        F.sum($"vehicle_count").as("vehicle_count_sum"),
        F.avg($"vehicle_count").as("vehicle_count_avg"),
        F.avg($"avg_speed").as("avg_speed_avg"),
        F.avg($"traffic_density").as("traffic_density_avg"),
        F.sum(F.when($"is_incident" === 1, 1).otherwise(0)).as("incidents")
      )

    val activeDays = enriched
      .withColumn("event_date", F.to_date($"event_ts"))
      .groupBy($"sensor_id")
      .agg(F.countDistinct($"event_date").as("active_days"))

    val report = bySensorAgg
      .join(activeDays, Seq("sensor_id"), "left")
      .withColumn("active_days", F.coalesce($"active_days", F.lit(0)))
      .withColumn("uptime_ratio_days", F.col("active_days") / F.lit(totalDays.toDouble))

    report
  }

  // ------------------------------------------------------------------------------------
  // 3) Analyse de cohortes de capteurs
  //    - premier mois d’activité du capteur (first_active_month)
  //    - rétention mensuelle : capteurs toujours actifs par mois depuis T0
  //    -> renvoie une table tidy (cohort, month_index, active_sensors)
  // ------------------------------------------------------------------------------------
  def sensorCohortAnalysis(enriched: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val withMonth = enriched
      .withColumn("event_month", F.date_format(F.col("event_ts"), "yyyy-MM"))
      .select($"sensor_id", $"event_month")
      .distinct()

    val firstMonthBySensor = withMonth
      .groupBy($"sensor_id")
      .agg(F.min($"event_month").as("first_active_month"))

    val cohort = withMonth
      .join(firstMonthBySensor, Seq("sensor_id"))
      .withColumn(
        "month_index",
        F.months_between(
          F.to_date(F.concat($"event_month", F.lit("-01"))),
          F.to_date(F.concat($"first_active_month", F.lit("-01")))
        ).cast("int")
      )
      .groupBy($"first_active_month".as("cohort_month"), $"month_index")
      .agg(F.countDistinct($"sensor_id").as("active_sensors"))
      .orderBy($"cohort_month", $"month_index")

    cohort
  }

  // ------------------------------------------------------------------------------------
  // 4) Heures de pointe par zone (optionnel mais utile au pilotage)
  //    - calcule l’heure la plus chargée (somme véhicules) et la vitesse moyenne associée
  // ------------------------------------------------------------------------------------
  def zonePeakHours(enriched: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val byZoneHour = enriched
      .groupBy($"zone_id", $"zone_name", $"district", $"hour")
      .agg(
        F.sum($"vehicle_count").as("veh_sum"),
        F.avg($"avg_speed").as("avg_speed_at_hour")
      )

    val w = Window.partitionBy($"zone_id").orderBy($"veh_sum".desc)
    byZoneHour
      .withColumn("rnk", F.row_number().over(w))
      .where($"rnk" === 1)
      .drop("rnk")
      .withColumnRenamed("hour", "peak_hour")
  }
}

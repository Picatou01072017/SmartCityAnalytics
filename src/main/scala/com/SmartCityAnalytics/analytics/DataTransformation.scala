// DataTransformation.scala
package com.SmartCityAnalytics.analytics

import com.SmartCityAnalytics.models._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object DataTransformation {

  // ----------------------------------------------------------------------------
  // Helpers & UDFs
  // ----------------------------------------------------------------------------

  /** Normalise un BigInt timestamp : ms -> s si nécessaire, puis -> Timestamp */
  private val toTs = F.udf { (ts: java.math.BigInteger) =>
    if (ts == null) null
    else {
      val v = ts.longValue()
      val seconds =
        if (v > 1000000000000L) v / 1000L     // heuristique: si > ~2001-09-09 en ms
        else v                                // déjà en secondes
      new java.sql.Timestamp(seconds * 1000L)
    }
  }

  case class TimeFeatures(
                           hour: Int,
                           dayOfWeek: String,
                           month: String,
                           isWeekend: Int,
                           dayPeriod: String,
                           isWorkingHour: Int
                         )

  /** Extrait des features temporelles à partir d'un Timestamp */
  private val extractTimeFeatures = F.udf { (ts: java.sql.Timestamp) =>
    try {
      val dt = ts.toLocalDateTime
      val hour = dt.getHour
      val dow = dt.getDayOfWeek.toString // MONDAY..SUNDAY
      val month = dt.getMonth.toString   // JANUARY..DECEMBER
      val isWeekend = if (dt.getDayOfWeek.getValue >= 6) 1 else 0
      val dayPeriod =
        if (hour >= 6 && hour < 12) "Morning"
        else if (hour >= 12 && hour < 18) "Afternoon"
        else if (hour >= 18 && hour < 22) "Evening"
        else "Night"
      val isWorkingHour = if (hour >= 9 && hour < 17) 1 else 0
      TimeFeatures(hour, dow, month, isWeekend, dayPeriod, isWorkingHour)
    } catch {
      case _: Throwable => TimeFeatures(0, "UNKNOWN", "UNKNOWN", 0, "UNKNOWN", 0)
    }
  }

  // ----------------------------------------------------------------------------
  // 1) Enrichissement des événements de trafic par capteurs, zones & météo
  // ----------------------------------------------------------------------------
  /**
   * - Convertit le timestamp (BigInt) en Timestamp
   * - Joint avec Sensors (sensor_id) puis CityZones (via zone_id)
   * - Joint avec Weather en choisissant la mesure météo la plus proche temporellement
   * - Ajoute des features temporelles + indicateurs de trafic
   */
  def enrichTraffic(
                     traffic: Dataset[TrafficEvents],
                     sensors: Dataset[Sensors],
                     zones: Dataset[CityZones],
                     weather: Dataset[Weather]
                   )(implicit spark: SparkSession): DataFrame = {

    // Events avec timestamp normalisé
    val te = traffic
      .withColumn("event_ts", toTs(F.col("timestamp")))
      .withColumn("event_epoch_s", F.col("event_ts").cast("long"))

    // Sensors + Zones
    val sz = sensors
      .join(zones.toDF(), Seq("zone_id"), "left")

    val joined = te
      .join(sz.toDF(), Seq("sensor_id"), "left")
      .withColumn("time_features", extractTimeFeatures(F.col("event_ts")))
      .withColumn("hour", F.col("time_features.hour"))
      .withColumn("day_of_week", F.col("time_features.dayOfWeek"))
      .withColumn("month", F.col("time_features.month"))
      .withColumn("is_weekend", F.col("time_features.isWeekend"))
      .withColumn("day_period", F.col("time_features.dayPeriod"))
      .withColumn("is_working_hour", F.col("time_features.isWorkingHour"))
      .drop("time_features")

    // Préparer Weather (timestamp -> ts + epoch)
    val wx = weather
      .withColumn("wx_ts", toTs(F.col("timestamp")))
      .withColumn("wx_epoch_s", F.col("wx_ts").cast("long"))
      .select(
        F.col("sensor_id").as("wx_sensor_id"),
        F.col("wx_ts"),
        F.col("wx_epoch_s"),
        F.col("temperature"),
        F.col("humidity"),
        F.col("pressure"),
        F.col("wind_speed"),
        F.col("rainfall")
      )

    // Match météo la plus proche par (sensor_id, |Δt| min)
    val joinedWx = joined
      .join(wx, F.col("sensor_id") === F.col("wx_sensor_id"), "left")
      .withColumn("time_diff", F.abs(F.col("event_epoch_s") - F.col("wx_epoch_s")))
      .withColumn("rn", F.row_number().over(
        Window.partitionBy(F.col("event_id")).orderBy(F.col("time_diff").asc_nulls_last)
      ))
      .where(F.col("rn") === 1)
      .drop("rn", "time_diff", "wx_sensor_id", "wx_epoch_s")

    // Indicateurs supplémentaires
    val enriched = joinedWx
      .withColumn(
        "is_incident",
        F.when(F.trim(F.col("incident_type")) =!= "" && F.col("incident_type").isNotNull, 1).otherwise(0)
      )
      .withColumn(
        "congestion_level",
        F.when(F.col("avg_speed") < 15.0 || F.col("traffic_density") >= 0.8, "Severe")
          .when(F.col("avg_speed") < 30.0 || F.col("traffic_density") >= 0.6, "High")
          .when(F.col("avg_speed") < 45.0 || F.col("traffic_density") >= 0.4, "Moderate")
          .otherwise("Low")
      )

    enriched
  }

  // ----------------------------------------------------------------------------
  // 2) KPIs par zone / heure / jour
  // ----------------------------------------------------------------------------
  def computeZoneKPIs(enriched: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val by = Seq("zone_id", "zone_name", "district", "hour", "day_of_week", "month", "is_weekend", "day_period", "is_working_hour")

    val agg = enriched
      .groupBy(by.map(F.col): _*)
      .agg(
        F.countDistinct(F.col("event_id")).as("events"),
        F.sum(F.col("vehicle_count")).as("vehicle_count_sum"),
        F.avg(F.col("vehicle_count")).as("vehicle_count_avg"),
        F.avg(F.col("avg_speed")).as("avg_speed_avg"),
        F.avg(F.col("traffic_density")).as("traffic_density_avg"),
        F.expr("percentile_approx(avg_speed, 0.5)").as("avg_speed_p50"),
        F.sum(F.when(F.col("is_incident") === 1, 1).otherwise(0)).as("incidents"),
        // météo agrégée sur la même granularité temporelle
        F.avg(F.col("temperature")).as("temperature_avg"),
        F.avg(F.col("humidity")).as("humidity_avg"),
        F.avg(F.col("pressure")).as("pressure_avg"),
        F.avg(F.col("wind_speed")).as("wind_speed_avg"),
        F.avg(F.col("rainfall")).as("rainfall_avg")
      )
      .withColumn("incident_rate", F.col("incidents") / F.col("events"))
      .withColumn(
        "congestion_score",
        F.least(
          F.lit(1.0),
          F.greatest(
            F.lit(0.0),
            F.lit(0.6) * F.col("traffic_density_avg") + F.lit(0.4) * (F.lit(1.0) - (F.col("avg_speed_avg") / F.lit(60.0)))
          )
        )
      )

    agg
  }

  // ----------------------------------------------------------------------------
  // 3) Rolling métriques par capteur (fenêtre temporelle)
  // ----------------------------------------------------------------------------
  def computeSensorRolling(enriched: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val w = Window
      .partitionBy(F.col("sensor_id"))
      .orderBy(F.col("event_ts").cast("long"))
      .rowsBetween(-20, 0) // ex: dernière ~vingtaine d’événements

    enriched
      .withColumn("veh_count_roll_avg", F.avg(F.col("vehicle_count")).over(w))
      .withColumn("speed_roll_avg",     F.avg(F.col("avg_speed")).over(w))
      .withColumn("density_roll_avg",   F.avg(F.col("traffic_density")).over(w))
      .withColumn("incident_roll_rate", F.avg(F.col("is_incident")).over(w))
  }

  // ----------------------------------------------------------------------------
  // 4) Activité capteurs (uptime, dernière activité, statut)
  // ----------------------------------------------------------------------------
  def computeSensorActivity(enriched: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val lastSeen = enriched
      .groupBy(F.col("sensor_id"))
      .agg(
        F.max(F.col("event_ts")).as("last_event_ts"),
        F.count(F.lit(1)).as("events_count"),
        F.sum(F.col("is_incident")).as("incidents_count")
      )

    // Statut déclaré vs. activité observée
    val statusBySensor = enriched
      .select(F.col("sensor_id"), F.col("status"))
      .dropDuplicates("sensor_id")

    lastSeen
      .join(statusBySensor, Seq("sensor_id"), "left")
      .withColumn("is_active_observed", F.when(F.col("events_count") > 0, 1).otherwise(0))
  }

  // ----------------------------------------------------------------------------
  // 5) Vue “tableau de bord” par zone (mix KPIs + rolling moyens par capteurs)
  // ----------------------------------------------------------------------------
  def buildZoneDashboard(
                          traffic: Dataset[TrafficEvents],
                          sensors: Dataset[Sensors],
                          zones: Dataset[CityZones],
                          weather: Dataset[Weather]
                        )(implicit spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
    val enriched = enrichTraffic(traffic, sensors, zones, weather)
    val zoneKpis = computeZoneKPIs(enriched)
    val sensorRolling = computeSensorRolling(enriched)
    val sensorActivity = computeSensorActivity(enriched)
    (zoneKpis, sensorRolling, sensorActivity)
  }
}

// src/main/scala/com/ecommerce/analytics/DataIngestion.scala
package com.ecommerce.analytics

import org.apache.spark.sql.{Dataset, SparkSession, functions => F}
import com.ecommerce.models._
import com.typesafe.config.ConfigFactory

object DataIngestion {
  private val config     = ConfigFactory.load()
  private val dataConfig = config.getConfig("app.data.input")

  private def report(name: String, total: Long, valid: Long): Unit =
    println(s"[$name] Lignes lues : $total | Lignes valides : $valid")

  // ---------------------------
  // CityZones (CSV avec header)
  // ---------------------------
  def readCityZones(spark: SparkSession): Dataset[CityZones] = {
    import spark.implicits._
    try {
      val path = dataConfig.getString("city_zones")
      val raw  = spark.read.option("header","true").option("inferSchema","true").csv(path)

      val total = raw.count()
      val ds = raw.select(
        F.col("zone_id").cast("string"),
        F.col("zone_name").cast("string"),
        F.col("zone_type").cast("string"),
        // BigInt ⇒ passer par Decimal(38,0)
        F.col("population").cast("decimal(38,0)").as("population"),
        F.col("area_km2").cast("double"),
        F.col("district").cast("string")
      ).as[CityZones]

      val valid = ds.filter(z =>
        Option(z.zone_id).exists(_.nonEmpty) &&
          Option(z.zone_name).exists(_.nonEmpty) &&
          z.population.bigInteger.signum() > 0 &&
          z.area_km2 > 0.0
      )

      report("CityZones", total, valid.count())
      valid
    } catch {
      case e: Exception =>
        println(s"Erreur CityZones : ${e.getMessage}"); spark.emptyDataset[CityZones]
    }
  }

  // ---------------------------
  // Sensors (JSON, champs déjà OK)
  // ---------------------------
  def readSensors(spark: SparkSession): Dataset[Sensors] = {
    import spark.implicits._
    try {
      val path = dataConfig.getString("sensors")
      val raw  = spark.read.option("multiline","true").json(path)

      val total = raw.count()
      val ds = raw.select(
        F.col("sensor_id").cast("string"),
        F.col("sensor_type").cast("string"),
        F.col("zone_id").cast("string"),
        F.col("installation_date").cast("string"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("status").cast("string")
      ).as[Sensors]

      val valid = ds.filter(s =>
        Option(s.sensor_id).exists(_.nonEmpty) &&
          Option(s.zone_id).exists(_.nonEmpty) &&
          s.latitude >= -90 && s.latitude <= 90 &&
          s.longitude >= -180 && s.longitude <= 180
      )

      report("Sensors", total, valid.count())
      valid
    } catch {
      case e: Exception =>
        println(s"Erreur Sensors : ${e.getMessage}"); spark.emptyDataset[Sensors]
    }
  }

  // ---------------------------
  // TrafficEvents (Parquet, champs déjà OK)
  // ---------------------------
  def readTrafficEvents(spark: SparkSession): Dataset[TrafficEvents] = {
    import spark.implicits._
    try {
      val path = dataConfig.getString("traffic_events")
      val raw  = spark.read.parquet(path)

      val total = raw.count()
      val ds = raw.select(
        F.col("event_id").cast("string"),
        F.col("sensor_id").cast("string"),
        // BigInt ⇒ via Decimal(38,0) (au cas où le parquet stocke string/long)
        F.col("timestamp").cast("decimal(38,0)").as("timestamp"),
        F.col("vehicle_count").cast("int"),
        F.col("avg_speed").cast("double"),
        F.col("traffic_density").cast("double"),
        F.col("incident_type").cast("string")
      ).as[TrafficEvents]

      val valid = ds.filter(te =>
        Option(te.event_id).exists(_.nonEmpty) &&
          Option(te.sensor_id).exists(_.nonEmpty) &&
          te.vehicle_count >= 0 &&
          te.avg_speed >= 0.0 &&
          te.traffic_density >= 0.0 && te.traffic_density <= 1.0
      )

      report("TrafficEvents", total, valid.count())
      valid
    } catch {
      case e: Exception =>
        println(s"Erreur TrafficEvents : ${e.getMessage}"); spark.emptyDataset[TrafficEvents]
    }
  }

  // ---------------------------
  // Weather (CSV avec header)
  // ---------------------------
  def readWeather(spark: SparkSession): Dataset[Weather] = {
    import spark.implicits._
    try {
      val path = dataConfig.getString("weather")
      val raw  = spark.read.option("header","true").option("inferSchema","true").csv(path)

      val total = raw.count()
      val ds = raw.select(
        F.col("reading_id").cast("string"),
        F.col("sensor_id").cast("string"),
        // BigInt ⇒ via Decimal(38,0)
        F.col("timestamp").cast("decimal(38,0)").as("timestamp"),
        F.col("temperature").cast("double"),
        F.col("humidity").cast("double"),
        F.col("pressure").cast("double"),
        F.col("wind_speed").cast("double"),
        F.col("rainfall").cast("double")
      ).as[Weather]

      val valid = ds.filter(w =>
        Option(w.reading_id).exists(_.nonEmpty) &&
          Option(w.sensor_id).exists(_.nonEmpty) &&
          w.humidity >= 0 && w.humidity <= 100 &&
          w.pressure > 500 && w.pressure < 1200 &&
          w.wind_speed >= 0 && w.rainfall >= 0
      )

      report("Weather", total, valid.count())
      valid
    } catch {
      case e: Exception =>
        println(s"Erreur Weather : ${e.getMessage}"); spark.emptyDataset[Weather]
    }
  }
}

// src/main/scala/com/SmartCityAnalytics/analytics/DataIngestion.scala
package com.SmartCityAnalytics.analytics

import org.apache.spark.sql.{Dataset, SparkSession, functions => F}
import com.SmartCityAnalytics.models._
import com.typesafe.config.ConfigFactory

object DataIngestion {
  private val config     = ConfigFactory.load()
  private val dataConfig = config.getConfig("app.data.input")

  private def report(name: String, total: Long, valid: Long): Unit =
    println(s"[$name] Lignes lues : $total | Lignes valides : $valid")

  // Helpers lisibles pour les Option[…]
  private def nonEmpty(s: String): Boolean = s != null && s.nonEmpty
  private def nonNeg(o: Option[Double]): Boolean = o.forall(_ >= 0.0)
  private def nonNegI(o: Option[Int]): Boolean = o.forall(_ >= 0)
  private def inRange(o: Option[Double], min: Double, max: Double): Boolean =
    o.forall(v => v >= min && v <= max)
  private def between(o: Option[Double], min: Double, max: Double): Boolean =
    o.forall(v => v > min && v < max)

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
        nonEmpty(z.zone_id) &&
          nonEmpty(z.zone_name) &&
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
  // Sensors (JSON)
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
        nonEmpty(s.sensor_id) &&
          nonEmpty(s.zone_id) &&
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
  // TrafficEvents (Parquet)
  // NOTE: ce code part du principe que les champs
  // vehicle_count / avg_speed / traffic_density sont Option[…]
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
        // BigInt ⇒ via Decimal(38,0)
        F.col("timestamp").cast("decimal(38,0)").as("timestamp"),
        F.col("vehicle_count").cast("int"),
        F.col("avg_speed").cast("double"),
        F.col("traffic_density").cast("double"),
        F.col("incident_type").cast("string")
      ).as[TrafficEvents]

      val valid = ds.filter(te =>
        nonEmpty(te.event_id) &&
          nonEmpty(te.sensor_id) &&
          nonNegI(te.vehicle_count) &&
          nonNeg(te.avg_speed) &&
          inRange(te.traffic_density, 0.0, 1.0)
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
  // NOTE: ce code part du principe que temperature,
  // humidity, pressure, wind_speed, rainfall sont Option[…]
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
        nonEmpty(w.reading_id) &&
          nonEmpty(w.sensor_id) &&
          inRange(w.humidity, 0.0, 100.0) &&
          between(w.pressure, 500.0, 1200.0) &&
          nonNeg(w.wind_speed) &&
          nonNeg(w.rainfall)
        // (optionnel) filtrage température plausible :
        // inRange(w.temperature, -60.0, 60.0)
      )

      report("Weather", total, valid.count())
      valid
    } catch {
      case e: Exception =>
        println(s"Erreur Weather : ${e.getMessage}"); spark.emptyDataset[Weather]
    }
  }
}

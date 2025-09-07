// MainApp.scala
package com.ecommerce.analytics

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.storage.StorageLevel
import com.typesafe.config.ConfigFactory

object MainApp {
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    val config = ConfigFactory.load()

    try {
      // Configuration Spark
      spark = SparkSession.builder()
        .appName(config.getString("app.spark.appName"))
        .master(config.getString("app.spark.master"))
        .getOrCreate()
    } catch {
      case e: Exception =>
        println(s"Erreur lors de la création de la session Spark : ${e.getMessage}")
        e.printStackTrace()
    }

    try {
      // -------------------------------------------------------------------
      // Lecture des datasets
      // -------------------------------------------------------------------
      val cityZones     = DataIngestion.readCityZones(spark)
      val sensors       = DataIngestion.readSensors(spark)
      val trafficEvents = DataIngestion.readTrafficEvents(spark)
      val weather       = DataIngestion.readWeather(spark)

      // -------------------------------------------------------------------
      // Transformation : enrichissement + calcul des indicateurs
      // -------------------------------------------------------------------
      val (zoneKpis, sensorRolling, sensorActivity) =
        DataTransformation.buildZoneDashboard(trafficEvents, sensors, cityZones, weather)(spark)

      // -------------------------------------------------------------------
      // Aperçu des résultats
      // -------------------------------------------------------------------
      println("=== KPIs par zone ===")
      zoneKpis.show(10, truncate = false)

      println("=== Rolling metrics par capteur ===")
      sensorRolling.show(10, truncate = false)

      println("=== Activité capteurs ===")
      sensorActivity.show(10, truncate = false)

      // -------------------------------------------------------------------
      // Sauvegarde
      // -------------------------------------------------------------------
      val outputPath = config.getString("app.data.output.path")
      save(zoneKpis, outputPath + "/zone_kpis")
      save(sensorRolling, outputPath + "/sensor_rolling")
      save(sensorActivity, outputPath + "/sensor_activity")

      println("✅ Application exécutée avec succès.")
    } catch {
      case e: Exception =>
        println(s"Erreur dans l'application principale : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      if (spark != null) spark.stop()
    }
  }

  def save(df: DataFrame, basePath: String): Unit = {
    df.write.mode("overwrite").csv(basePath + "/csv")
    // df.write.mode("overwrite").parquet(basePath + "/parquet")
  }
}

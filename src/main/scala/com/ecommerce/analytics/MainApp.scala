package com.ecommerce.analytics

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.storage.StorageLevel
import com.typesafe.config.ConfigFactory
import java.io.File

object MainApp {
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
     val config = ConfigFactory.load()
     try{
       // Configurations
       spark = SparkSession.builder()
         .appName(config.getString("app.spark.appName"))
         .master(config.getString("app.spark.master"))
         .getOrCreate()
     }catch {
       case e: Exception =>
         println(s"Une erreur s'est produite lors de la création de la session spark : ${e.getMessage}")
         e.printStackTrace()
     }
    try {

      // Lecture des datasets
      val transactions = DataIngestion.readTransactions(spark)
//        .cache()
      val users = DataIngestion.readUsers(spark)
//        .cache()
      val merchants = DataIngestion.readMerchants(spark)
//        .cache()
//      val products = DataIngestion.readProducts(spark).cache()

      // Enrichissement avec persist
      val enriched = DataTransformation
        .enrichTransactionData(transactions, users, merchants)(spark)
//        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      enriched.show(10)

      // Activité utilisateur
      val userActivity = DataTransformation.computeUserActivity(transactions)(spark)
      userActivity.show(false)

      // Rapport par marchand
      val merchantReport = Analytics.merchantKPIReport(enriched)(spark)
      merchantReport.show(false)

      // Analyse de fidelite
      val cohortReport = Analytics.userCohortAnalysis(enriched)(spark)
      cohortReport.show(false)

      // Sauvegarde des résultats
      val outputPath = config.getString("app.data.output.path")
//      save(userActivity, outputPath + "/user_activity_report")
//      save(merchantReport, outputPath + "/merchant_report")
//      save(cohortReport, outputPath + "/cohort_report")

      // Libérer la mémoire
      enriched.unpersist()
      println(s"L'application s'est bien exécutée correctement.")
    } catch {
      case e: Exception =>
        println(s"Erreur dans l'application principale : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  def save(df: DataFrame, basePath: String): Unit = {
    df.write.mode("overwrite").csv(basePath + "/csv")
//    df.write.mode("overwrite").parquet(basePath + "/parquet")
  }
}

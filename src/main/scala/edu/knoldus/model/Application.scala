package edu.knoldus.model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Application extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("footballAnalysis")

  val spark: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val analyzer = FootballAnalysis(spark)
  val footballData = analyzer.getDataFrameOf(FILE_LOCATION)
  analyzer.countMatchesByEachTeam(footballData).show(NUMBER)
  analyzer.mostWinPercentage(footballData).show()
  val footballDataset = analyzer.getDatasetFrom(footballData)
  analyzer.topTenTeam(footballDataset).show

}

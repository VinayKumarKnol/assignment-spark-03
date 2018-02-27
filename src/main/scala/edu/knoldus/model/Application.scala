package edu.knoldus.model

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Application extends App {

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("footballAnalysis")

  val spark: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val analyzer = FootballAnalysis(spark)
  val footballData = analyzer.getDataFrameOf(FILE_LOCATION)
//  analyzer.countMatchesByEachTeam(footballData).show(17)
  analyzer.mostWinPercentage(footballData).show()

}
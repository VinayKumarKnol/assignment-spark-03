package edu.knoldus.model

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FootballAnalysis(spark: SparkSession) {

  def getDataFrameOf(url: String): DataFrame = {
    spark.read
      .option("header", "true")
      .csv(url)
  }

  def countMatchesByEachTeam(footballData: DataFrame): DataFrame = {
    footballData
      .select("HomeTeam")
      .withColumn("count", lit(1))
      .groupBy("HomeTeam")
      .sum("count")
      .sort(desc("SUM(count)"))
  }

  def mostWinPercentage(footballData: DataFrame): DataFrame = {
    footballData
      .select("HomeTeam", "AwayTeam", "FTR")
      .createOrReplaceTempView("matches")
    val homeWins = spark.sql(
      "select HomeTeam, sum(case when FTR = 'H' then 1 else 0 end) as homeWins" +
        ", count(*)  as totalMatches" +
        " from matches " +
        " group by HomeTeam"
    )
    val awayWins = spark.sql(
      "select AwayTeam, sum(case when FTR = 'A' then 1 else 0 end) as awayWins " +
        ", count(*) as totalMatches " +
        " from matches " +
        " group by AwayTeam "
    )

    homeWins.createOrReplaceTempView("homeWinsView")
    awayWins.createOrReplaceTempView("awayWinsView")

    val result = spark.sql(
      "select HomeTeam ," +
        " (homeWins + awayWins) * 100 / (homeWinsView.totalMatches + awayWinsView.totalMatches) as win_percentage " +
        " from homeWinsView join awayWinsView on homeWinsView.HomeTeam = awayWinsView.AwayTeam " +
        " order by win_percentage DESC " +
        " limit 10")
    result
  }
}
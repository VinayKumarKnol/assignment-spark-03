package edu.knoldus.model

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class FootballAnalysis(spark: SparkSession) {

  val dFToProcess: Encoder[FootballMatch] = Encoders.kryo[FootballMatch]

  val footballMatchEncoder: Encoder[FootballMatch] = Encoders.product[FootballMatch]

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
        " round((homeWins + awayWins) * 100 / (homeWinsView.totalMatches + awayWinsView.totalMatches), 2) as win_percentage " +
        " from homeWinsView join awayWinsView on homeWinsView.HomeTeam = awayWinsView.AwayTeam " +
        " order by win_percentage DESC " +
        " limit 10")
    result
  }

  def getDatasetFrom(footballDataFrame: DataFrame): Dataset[FootballMatch] = {
    import spark.implicits._
    footballDataFrame
      .map {
        row => {
          FootballMatch(
            row.getAs[String]("HomeTeam"),
            row.getAs[String]("AwayTeam"),
            row.getAs[String]("FTHG").toInt,
            row.getAs[String]("FTAG").toInt,
            row.getAs[String]("FTR")
          )
        }
      }
  }

  def topTenTeam(footballDataset: Dataset[FootballMatch]): DataFrame = {
    footballDataset
      .select("homeTeam", "awayTeam", "result")
      .createOrReplaceTempView("matches")

    val homeWins = spark.sql(
      "select homeTeam, sum(case when result = 'H' then 1 else 0 end) as homeWins" +
        ", count(*)  as totalMatches" +
        " from matches " +
        " group by HomeTeam"
    )

    val awayWins = spark.sql(
      "select awayTeam, sum(case when result = 'A' then 1 else 0 end) as awayWins " +
        ", count(*) as totalMatches " +
        " from matches " +
        " group by AwayTeam "
    )

    homeWins.createOrReplaceTempView("homeWinsView")
    awayWins.createOrReplaceTempView("awayWinsView")

    val result = spark.sql(
      "select homeTeam ," +
        " round((homeWins + awayWins) * 100 / (homeWinsView.totalMatches + awayWinsView.totalMatches), 2) as win_percentage " +
        " from homeWinsView join awayWinsView on homeWinsView.HomeTeam = awayWinsView.AwayTeam " +
        " order by win_percentage DESC " +
        " limit 10")
    result.select("homeTeam")
  }
}

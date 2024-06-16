package com.jmx.metrics.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 需求1：查找电影评分个数超过5000,且平均评分较高的前十部电影名称及其对应的平均评分
  */
class BestFilmsByOverallRating extends Serializable {
  def run(moviesDataset: DataFrame, ratingsDataset: DataFrame, spark: SparkSession): Unit = {
    val startTime = System.currentTimeMillis()

    val ratingsStatsDF = ratingsDataset
      .groupBy("movieId")
      .agg(
        count("rating").as("rating_cnt"),
        avg("rating").as("avg_rating")
      )
      .filter("rating_cnt >= 5000")

    val topMoviesDF = ratingsStatsDF
      .orderBy(desc("avg_rating"))
      .limit(10)

    val resultDF = topMoviesDF
      .join(moviesDataset, "movieId")
      .select("movieId", "title", "avg_rating")

    resultDF.show(10)
    resultDF.printSchema()

    val endTime = System.currentTimeMillis()
    val elapsedTime = endTime - startTime
    println(s"BestFilmsByOverallRating 完成，总共用时: $elapsedTime 毫秒")
  }
}

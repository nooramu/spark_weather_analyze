package com.jmx.metrics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 需求3：查找被评分次数较多的前十部电影
  */
class MostRatedFilms extends Serializable {
  def run(moviesDataset: DataFrame, ratingsDataset: DataFrame, spark: SparkSession): Unit = {
    val startTime = System.currentTimeMillis()

    val mostRatedMoviesDF = ratingsDataset
      .groupBy("movieId")
      .agg(count("rating").as("rating_count"))
      .orderBy(desc("rating_count"))
      .limit(10)

    val resultDF = mostRatedMoviesDF
      .join(moviesDataset, "movieId")
      .select("movieId", "title", "rating_count")

    resultDF.show(10)
    resultDF.printSchema()

    val endTime = System.currentTimeMillis()
    val elapsedTime = endTime - startTime
    println(s"MostRatedFilms 完成，总共用时: $elapsedTime 毫秒")
  }
}

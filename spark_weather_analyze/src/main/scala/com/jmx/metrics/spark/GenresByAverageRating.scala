package com.jmx.metrics.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 需求2：查找每个电影类别及其对应的平均评分
  */
class GenresByAverageRating extends Serializable {
  def run(moviesDataset: DataFrame, ratingsDataset: DataFrame, spark: SparkSession): Unit = {
    val startTime = System.currentTimeMillis()

    val explodedMoviesDF = moviesDataset
      .withColumn("genre", explode(split(col("genres"), "\\|")))

    val genresAvgRatingDF = explodedMoviesDF
      .join(ratingsDataset, "movieId")
      .groupBy("genre")
      .agg(avg("rating").as("avg_rating"))

    genresAvgRatingDF.show(10)
    genresAvgRatingDF.printSchema()

    val endTime = System.currentTimeMillis()
    val elapsedTime = endTime - startTime
    println(s"GenresByAverageRating 完成，总共用时: $elapsedTime 毫秒")
  }
}

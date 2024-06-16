package com.jmx.metrics.shujuku

import com.jmx.demos.tenMostRatedFilms
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *  @Created with IntelliJ IDEA.
  *  @author : jmx
  *  @Date: 2020/11/19
  *  @Time: 15:23
  *  */

/**
  * 需求3：查找被评分次数较多的前十部电影.
  */
class MostRatedFilms extends Serializable {
  def run(moviesDataset: DataFrame, ratingsDataset: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._

    // Start timing
    val startTime = System.currentTimeMillis()

    // 将moviesDataset注册成表
    moviesDataset.createOrReplaceTempView("movies")
    // 将ratingsDataset注册成表
    ratingsDataset.createOrReplaceTempView("ratings")

    val ressql3 =
      """
        |WITH rating_group AS (
        |    SELECT
        |       movieId,
        |       count(*) AS ratingCnt
        |    FROM ratings
        |    GROUP BY movieId
        |),
        |rating_filter AS (
        |    SELECT
        |       movieId,
        |       ratingCnt
        |    FROM rating_group
        |    ORDER BY ratingCnt DESC
        |    LIMIT 10
        |)
        |SELECT
        |    m.movieId,
        |    m.title,
        |    r.ratingCnt
        |FROM
        |    rating_filter r
        |JOIN movies m ON r.movieId = m.movieId
      """.stripMargin

    val resultDS = spark.sql(ressql3).as[tenMostRatedFilms]
    // 打印数据到控制台
    resultDS.show(10)
    resultDS.printSchema()

    // Stop timing
    val endTime = System.currentTimeMillis()
    val totalTime = endTime - startTime
    println(s"Total execution time: $totalTime milliseconds")
  }
}

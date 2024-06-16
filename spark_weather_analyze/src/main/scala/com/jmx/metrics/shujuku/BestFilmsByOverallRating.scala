package com.jmx.metrics.shujuku

import com.jmx.demos.tenGreatestMoviesByAverageRating
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 需求1：查找电影评分个数超过5000,且平均评分较高的前十部电影名称及其对应的平均评分
  */
class BestFilmsByOverallRating extends Serializable {

  def run(moviesDataset: DataFrame, ratingsDataset: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._

    // 将moviesDataset注册成表
    moviesDataset.createOrReplaceTempView("movies")
    // 将ratingsDataset注册成表
    ratingsDataset.createOrReplaceTempView("ratings")

    // 记录开始时间
    val startTime = System.currentTimeMillis()

    // 查询SQL语句
    val ressql1 =
      """
        |WITH ratings_filter_cnt AS (
        |  SELECT
        |    movieId,
        |    count(*) AS rating_cnt,
        |    avg(rating) AS avg_rating
        |  FROM
        |    ratings
        |  GROUP BY
        |    movieId
        |  HAVING
        |    count(*) >= 5000
        |),
        |ratings_filter_score AS (
        |  SELECT
        |    movieId,
        |    avg_rating
        |  FROM ratings_filter_cnt
        |  ORDER BY avg_rating DESC
        |  LIMIT 10
        |)
        |SELECT
        |  m.movieId,
        |  m.title,
        |  r.avg_rating AS avgRating
        |FROM
        |  ratings_filter_score r
        |JOIN movies m ON m.movieId = r.movieId
      """.stripMargin

    val resultDS = spark.sql(ressql1).as[tenGreatestMoviesByAverageRating]
    // 打印数据到控制台
    resultDS.show(10)
    resultDS.printSchema()

    // 记录结束时间
    val endTime = System.currentTimeMillis()
    val elapsedTime = endTime - startTime
    println(s"数据处理完成，总共用时: $elapsedTime 毫秒")
  }
}

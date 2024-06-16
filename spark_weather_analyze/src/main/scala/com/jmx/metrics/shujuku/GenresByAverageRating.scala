package com.jmx.metrics.shujuku

import com.jmx.demos.topGenresByAverageRating
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 需求2：查找每个电影类别及其对应的平均评分
  */
class GenresByAverageRating extends Serializable {
  def run(moviesDataset: DataFrame, ratingsDataset: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._

    // 记录开始时间
    val startTime = System.currentTimeMillis()

    // 将moviesDataset注册成表
    moviesDataset.createOrReplaceTempView("movies")
    // 将ratingsDataset注册成表
    ratingsDataset.createOrReplaceTempView("ratings")

    val ressql2 =
      """
        |WITH explode_movies AS (
        |  SELECT
        |    movieId,
        |    title,
        |    category
        |  FROM
        |    movies
        |  LATERAL VIEW explode ( split ( genres, "\\|" ) ) temp AS category
        |)
        |SELECT
        |  m.category AS genres,
        |  avg(r.rating) AS avgRating
        |FROM
        |  explode_movies m
        |  JOIN ratings r ON m.movieId = r.movieId
        |GROUP BY
        |  m.category
      """.stripMargin

    val resultDS = spark.sql(ressql2).as[topGenresByAverageRating]

    // 打印数据
    resultDS.show(10)
    resultDS.printSchema()

    // 记录结束时间
    val endTime = System.currentTimeMillis()
    val elapsedTime = endTime - startTime
    println(s"数据处理完成，总共用时: $elapsedTime 毫秒")
  }

}

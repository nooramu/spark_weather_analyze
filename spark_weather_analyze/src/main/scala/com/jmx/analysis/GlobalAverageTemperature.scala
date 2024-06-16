package com.jmx.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class GlobalAverageTemperature extends Serializable {
  def calculate(dataWithSeason: DataFrame, spark: SparkSession): Future[Unit] = Future {
    val startTime = System.currentTimeMillis()

    // 计算全球平均气温（摄氏度）
    val globalAvgTemp = dataWithSeason.agg(avg("TEMP_Celsius").alias("Global_Avg_Temp_Celsius")).first().getDouble(0)
    println(s"全球平均气温 (摄氏度): $globalAvgTemp")

    val endTime = System.currentTimeMillis()
    val elapsedTimeInSeconds = (endTime - startTime) / 1000.0
    println(s"GlobalAverageTemperature 完成，总共用时: $elapsedTimeInSeconds 秒")
  }
}

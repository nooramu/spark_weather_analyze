package com.jmx.analysis

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class AverageTemperatureBySeason extends Serializable {
  def calculate(dataWithSeason: DataFrame, spark: SparkSession): Future[Unit] = Future {
    val startTime = System.currentTimeMillis()

    // 计算每个季节的平均温度
    val averageTempBySeason = dataWithSeason.groupBy("Season")
      .agg(avg("TEMP_Celsius").alias("Average_Temp_Celsius"))

    averageTempBySeason.orderBy("Season").show(1000, false)
    println("各季节平均气温计算完成。")

    val endTime = System.currentTimeMillis()
    val elapsedTimeInSeconds = (endTime - startTime) / 1000.0
    println(s"AverageTemperatureBySeason 完成，总共用时: $elapsedTimeInSeconds 秒")
  }
}

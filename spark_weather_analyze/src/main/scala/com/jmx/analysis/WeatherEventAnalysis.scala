package com.jmx.analysis

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class WeatherEventAnalysis extends Serializable {
  def run(dataDF: DataFrame, spark: SparkSession): Future[Unit] = Future {
    val startTime = System.currentTimeMillis()

    // 定义天气条件及其对应的索引
    val weatherConditions = Seq(
      "霜冻" -> 0,
      "雨" -> 1,
      "雪" -> 2,
      "冰雹" -> 3,
      "雷暴" -> 4,
      "龙卷风" -> 5
    )

    // 构建查询表达式
    val conditionsExpr = weatherConditions.map { case (condition, index) =>
      sum(when(substring(col("FRSHTT"), index + 1, 1) === 1, 1).otherwise(0)) as condition
    }

    // 使用一次性的聚合操作统计每种天气条件的出现次数
    val weatherCountsDF = dataDF.agg(conditionsExpr.head, conditionsExpr.tail: _*)

    // 打印结果
    weatherCountsDF.show()

    val endTime = System.currentTimeMillis()
    val elapsedTimeInSeconds = (endTime - startTime) / 1000.0
    println(s"WeatherEventAnalysis 完成，总共用时: $elapsedTimeInSeconds 秒")
  }
}

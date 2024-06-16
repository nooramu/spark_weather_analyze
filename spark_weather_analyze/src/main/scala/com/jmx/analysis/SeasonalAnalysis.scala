package com.jmx.analysis

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class SeasonalAnalysis extends Serializable {

  def run(dataWithSeason: DataFrame, spark: SparkSession): Future[Unit] = Future {
    val startTime = System.currentTimeMillis()

    // 按气象站和季节分组计算平均气温和降水量
    val seasonalStats = dataWithSeason.groupBy("NAME", "LATITUDE", "LONGITUDE", "Season")
      .agg(
        avg("TEMP_Celsius").alias("Avg_Temp_Celsius"),
        avg("PRCP").alias("Avg_Precipitation")
      )
      .select(
        col("NAME"),
        col("LATITUDE").cast(DoubleType).alias("Latitude"),
        col("LONGITUDE").cast(DoubleType).alias("Longitude"),
        col("Season"),
        col("Avg_Temp_Celsius"),
        col("Avg_Precipitation")
      )

    // 更精细的坐标范围来判断所在地区
    val determineContinentUDF: UserDefinedFunction = udf((latitude: Double, longitude: Double) => {
      if (latitude >= 24.396308 && latitude <= 49.384358 && longitude >= -125.0 && longitude <= -66.93457) "北美洲"
      else if (latitude <= -60 && latitude >= -90 && longitude >= -180 && longitude <= 180) "南极洲"
      else if (latitude >= 35 && latitude <= 71 && longitude >= -10 && longitude <= 60) "欧洲"
      else if (latitude >= -35 && latitude <= 35 && longitude >= -18 && longitude <= 52) "非洲"
      else if (latitude >= -56 && latitude <= 13 && longitude >= -81 && longitude <= -35) "南美洲"
      else if (latitude >= -50 && latitude <= 10 && longitude >= 110 && longitude <= 180) "大洋洲"
      else if (latitude >= -10 && latitude <= 80 && longitude >= 60 && longitude <= 180) "亚洲"
      else "未知"
    })

    // 根据经度和纬度信息判断所在地区，并添加地区列
    val statsWithRegion = seasonalStats.withColumn("Region", determineContinentUDF(col("Latitude"), col("Longitude")))
      .drop("Latitude", "Longitude")

    // 显示按气象站、季节和大洲分组的平均气温和降水量结果
    statsWithRegion.orderBy("Region", "NAME", "Season").show(false)
    println("按气象站、季节和大洲分组的平均气温和降水量计算完成。")

    // 按大陆和季节分组计算平均气温
    statsWithRegion.createOrReplaceTempView("statsWithRegion")

    val avgTempByRegionAndSeason = spark.sql(
      """
        |SELECT Region, Season, AVG(Avg_Temp_Celsius) AS Avg_Temp_Celsius
        |FROM statsWithRegion
        |GROUP BY Region, Season
        |ORDER BY Region, Season
      """.stripMargin)

    // 显示结果
    avgTempByRegionAndSeason.show(1000, false)
    println("按区域和季节分组计算平均气温完成。")

    val endTime = System.currentTimeMillis()
    val elapsedTimeInSeconds = (endTime - startTime) / 1000.0
    println(s"SeasonalAnalysis 完成，总共用时: $elapsedTimeInSeconds 秒")
  }

}

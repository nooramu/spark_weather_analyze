package com.jmx.demos

import com.jmx.analysis.{GlobalAverageTemperature, AverageTemperatureBySeason, SeasonalAnalysis, WeatherEventAnalysis}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import scala.concurrent.duration._
import org.apache.spark.sql.functions._
import scala.concurrent.{Await, ExecutionContext, Future}

object DemoMainApp {
  // 文件路径
  private val DATA_CSV_FILE_PATH = "file:///E:/Dasan/bigdata/data/combined_file.csv"

  // 创建一个全局执行上下文
  implicit val ec: ExecutionContext = ExecutionContext.global

  def main(args: Array[String]): Unit = {
    // 创建spark session
    val spark = SparkSession
      .builder
      .master("local[16]")
      .appName("Weather Analysis")
      .config("spark.sql.autoBroadcastJoinThreshold", "209715200") // 增加广播变量大小限制
      .config("spark.executor.memory", "16g") // 16增加执行内存
      .config("spark.driver.memory", "8g") // 8增加驱动内存
      .config("spark.executor.instances", "4") // 4个执行器
      .config("spark.executor.cores", "2") // 每个执行器使用2个核心
      .config("spark.local.dir", "E:/Dasan/bigdata/data/temp") // 使用本地磁盘存储
      .config("spark.sql.shuffle.partitions", "64") // 64增加分区数，优化并行度
      .config("spark.default.parallelism", "8") // 8个并行任务
      .getOrCreate()

    try {
      // 读取数据集
      val dataDF = readCsvIntoDataSet(spark, DATA_CSV_FILE_PATH)

      dataDF.printSchema()

      // 将华氏温度转换为摄氏温度并确定季节
      val fahrenheitToCelsiusUDF = udf((tempF: Double) => (tempF - 32) * 5 / 9)
      val seasons = Map(
        "Spring" -> List(3, 4, 5),
        "Summer" -> List(6, 7, 8),
        "Autumn" -> List(9, 10, 11),
        "Winter" -> List(12, 1, 2)
      )
      val dataWithSeason = dataDF.withColumn("TEMP_Celsius", fahrenheitToCelsiusUDF(col("TEMP")))
        .withColumn("Month", month(col("DATE")))
        .withColumn("Season",
          when(col("Month").isin(seasons("Spring"):_*), "Spring")
            .when(col("Month").isin(seasons("Summer"):_*), "Summer")
            .when(col("Month").isin(seasons("Autumn"):_*), "Autumn")
            .when(col("Month").isin(seasons("Winter"):_*), "Winter")
            .otherwise("Unknown")
        ).cache()  // 缓存中间结果

      // 并行执行任务
      println("开始执行分析任务...")
      val startTime = System.currentTimeMillis()

      // 创建各个分析任务的实例
      val globalAnalysis = new GlobalAverageTemperature
      val seasonalAnalysis = new SeasonalAnalysis
      val weatherEventAnalysis = new WeatherEventAnalysis
      val averageTemperatureBySeason = new AverageTemperatureBySeason

      // 分别以 Future 形式并行执行各个分析任务
      val futureGlobalAnalysis = globalAnalysis.calculate(dataWithSeason, spark)
      val futureSeasonalAnalysis = seasonalAnalysis.run(dataWithSeason, spark)
      val futureWeatherEventAnalysis = weatherEventAnalysis.run(dataWithSeason, spark)
      val futureAverageTemperatureBySeason = averageTemperatureBySeason.calculate(dataWithSeason, spark)

      // 等待所有任务完成
      val allFutures = Future.sequence(Seq(
        futureGlobalAnalysis,
        futureSeasonalAnalysis,
        futureWeatherEventAnalysis,
        futureAverageTemperatureBySeason
      ))

      // 处理所有任务结果
      Await.result(allFutures, Duration.Inf)

      val endTime = System.currentTimeMillis()
      val elapsedTime = (endTime - startTime) / 1000.0
      println(s"执行分析任务总用时: $elapsedTime 秒")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      spark.close()
    }
  }

  def readCsvIntoDataSet(spark: SparkSession, path: String, schema: StructType = null): DataFrame = {
    if (schema != null) {
      spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(path)
    } else {
      spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    }
  }
}

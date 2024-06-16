name := "WeatherAndPollutionAnalysis"

version := "0.1"

scalaVersion := "2.12.10"

// 添加项目依赖项
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "com.maxmind.geoip2" % "geoip2" % "3.0.1"
)

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven/"

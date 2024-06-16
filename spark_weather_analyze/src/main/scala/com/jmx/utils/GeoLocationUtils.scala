package com.jmx.utils

import java.io.File
import java.net.{InetAddress, UnknownHostException}

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.exception.GeoIp2Exception
import com.maxmind.geoip2.model.CityResponse

object GeoLocationUtils extends App {

  // 创建 GeoIP2 数据库阅读器
  val database = new File("src/main/resources/GeoLite2-City.mmdb")
  val reader = new DatabaseReader.Builder(database).build()

  // 示例：给定经度和纬度
  val latitude = 40.7128
  val longitude = -74.0060

  // 获取位置信息
  val geoInfo = getGeoInfo(latitude, longitude)

  // 输出结果
  geoInfo match {
    case Some(country) =>
      println(s"经度: $longitude, 纬度: $latitude")
      println(s"所属国家: $country")
    case None =>
      println("无法获取位置信息")
  }

  def getGeoInfo(latitude: Double, longitude: Double): Option[String] = {
    try {
      // 查询位置信息
      val ipAddress = InetAddress.getByAddress(Array[Byte](0, 0, 0, 0))
      val response: CityResponse = reader.city(ipAddress)

      // 获取国家信息
      val country = response.getCountry.getName

      Some(country)
    } catch {
      case _: UnknownHostException | _: GeoIp2Exception | _: java.io.IOException =>
        None
    }
  }
}

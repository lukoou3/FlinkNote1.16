package com.flink.utils

import org.scalatest.funsuite.AnyFunSuite

import JsonUtilSuite._

class JsonUtilSuite extends AnyFunSuite{

  // 是毫秒时间戳
  test("java.sql.Timestamp"){
    val data = RstDbData(1, 2)
    val json = JsonUtil.writeScValueAsString(data)
    println(json)
  }

}

object JsonUtilSuite{
  case class RstDbData(
    paimai_id: Long,
    cluster_id: java.lang.Long,
    create_time: java.sql.Timestamp = new java.sql.Timestamp(System.currentTimeMillis())
  )
}

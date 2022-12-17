package com.flink.debug.stream.base

import org.apache.flink.table.data.binary.BinaryStringData
import org.scalatest.funsuite.AnyFunSuite

class TestSuite extends AnyFunSuite{

  test("str"){
    val str = """aaa\sbbb无法无天111"""
    val bs = str.getBytes("utf-8")
    println(bs.toList)
    println("无法无天111".getBytes("utf-8").toList)
  }

  test("BinaryStringData"){
    val str = """aaa\sbbb无法无天111"""
    val bs = str.getBytes("utf-8")
    val stringData = BinaryStringData.fromBytes(bs)
    val strData = BinaryStringData.fromBytes("无法无天".getBytes("utf-8"))
    /**
     * 就是比较字节，先是8个字节8个字节比较，之后1个字节一个字节比较
     * spark的也差不多：
     * org.apache.spark.unsafe.types.UTF8String#contains
     */
    val contains = stringData.contains(strData)
    println(contains)
  }

}

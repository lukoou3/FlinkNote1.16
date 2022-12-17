package com.flink.utils

import java.text.SimpleDateFormat

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}


object JsonUtil {
  lazy val javaMapper = {
    /**
     * ScalaObjectMapper is deprecated because Manifests are not supported in Scala3
     * 这还早这呢, 现在flink使用的scala 2.11, => 2.12 => 2.13 => Scala3
     * 之后可以这样初始化(官方文档):
     * val mapper = JsonMapper.builder().addModule(DefaultScalaModule).build() :: ClassTagExtensions
     */
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
    mapper
  }
  lazy val scMapper = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
    mapper
  }

  def readValue[T](content: String, cls: Class[T]): T ={
    javaMapper.readValue(content, cls)
  }

  def readValue[T](content: Array[Byte], cls: Class[T]): T ={
    javaMapper.readValue(content, cls)
  }

  def readValue[T: Manifest](content: String): T ={
    javaMapper.readValue(content)
  }

  def writeValueAsString(value: AnyRef): String ={
    javaMapper.writeValueAsString(value)
  }

  def readScValue[T](content: String, cls: Class[T]): T ={
    scMapper.readValue(content, cls)
  }

  def readScValue[T](content: Array[Byte], cls: Class[T]): T ={
    scMapper.readValue(content, cls)
  }

  def readScValue[T: Manifest](content: String): T ={
    scMapper.readValue(content)
  }

  def writeScValueAsString(value: AnyRef): String ={
    scMapper.writeValueAsString(value)
  }
}

package com.flink.sql.func

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable.ArrayBuffer

class FileSource(filePath: String, sleep:Long = 10) extends RichParallelSourceFunction[String] {
  val lines = ArrayBuffer[String]()
  var stop = false
  var i = 0

  override def open(parameters: Configuration): Unit = {
    scala.io.Source.fromFile(filePath, "utf-8").getLines().filter(_.trim != "").foreach(lines += _.trim)
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val step = 1
    while (!stop) {
      if (i >= lines.size) {
        Thread.sleep(100000)
        i = 0
      }

      try{
        ctx.collect(lines(i))
      }catch {
        case e: Exception =>
          //e.printStackTrace()
          println("#" * 100)
          throw e
      }

      i += step

      try {
        Thread.sleep(sleep)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

  }

  override def cancel(): Unit = {
    this.stop = true
  }
}



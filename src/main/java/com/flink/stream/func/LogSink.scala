package com.flink.stream.func

import com.flink.log.Logging
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class LogSink[T] extends RichSinkFunction[T] with Logging{
  override def invoke(value: T, context: SinkFunction.Context): Unit = {
    logInfo(value.toString)
  }
}



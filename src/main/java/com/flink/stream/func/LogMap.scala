package com.flink.stream.func

import com.flink.log.Logging
import org.apache.flink.api.common.functions.MapFunction

class LogMap[T](prefix: String, logIntervalMs: Long = 0L) extends MapFunction[T, T] with Logging {
  var lastLogWriteTs = 0L
  val logPrefix = prefix + ":"
  def map(value: T): T = {
    if(logIntervalMs > 0L){
      val ts = System.currentTimeMillis()
      if(ts - lastLogWriteTs >= logIntervalMs){
        logInfo(logPrefix + value.toString)
        lastLogWriteTs = ts
      }
    }else{
      logInfo(logPrefix + value.toString)
    }
    value
  }
}

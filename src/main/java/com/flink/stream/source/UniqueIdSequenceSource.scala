package com.flink.stream.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class UniqueIdSequenceSource(val sleepMillis: Long = 100L) extends RichParallelSourceFunction[Long]{
  var stop = false
  var n = 0
  var k = 0

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    n = getRuntimeContext.getNumberOfParallelSubtasks
    k = getRuntimeContext.getIndexOfThisSubtask
  }

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    try {
      var i = k.toLong
      val step = n
      while (!stop) {
        ctx.collect(i)
        i += step
        Thread.sleep(sleepMillis)
      }
    } catch {
      case e:Exception =>
        e.printStackTrace()
    }
  }

  override def cancel(): Unit = {
    this.stop = true
  }
}

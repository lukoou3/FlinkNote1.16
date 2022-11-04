package com.flink.stream.source

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class IdSequenceSource(val sleepMillis: Long = 100L) extends RichParallelSourceFunction[(Int, Long)] {
  var stop = false
  @transient lazy val idx = getRuntimeContext.getIndexOfThisSubtask

  def run(ctx: SourceFunction.SourceContext[(Int, Long)]): Unit = {
    try {
      var i = 0
      val step = 1
      while (!stop) {
        ctx.collect((idx, i))
        i += step
        Thread.sleep(sleepMillis)
      }
    } catch {
      case e:Exception =>
        e.printStackTrace()
    }
  }

  def cancel(): Unit = {
    this.stop = true
  }
}

package com.flink.stream.utils

import com.fasterxml.jackson.databind.ObjectReader
import com.flink.stream.func.{LogMap, LogSink}
import com.flink.utils.JsonUtil
import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.Collector

object DataStreamImplicits {

  implicit class DataStreamOps[T](ds: DataStream[T]) {
    // 省不了多少代码, 官方KeyedStream的mapWithState这种情况省的多
    def mapWithData[D, R: TypeInformation](createData: => D)(func: (T, D) => R): DataStream[R] = {
      ds.map(new RichMapFunction[T, R] {
        @transient lazy val data: D = createData

        override def map(value: T): R = {
          func(value, data)
        }
      })
    }

    def flatMapWithData[D, R: TypeInformation](createData: => D)(func: (T, D) => TraversableOnce[R]): DataStream[R] = {
      ds.flatMap(new FlatMapFunction[T, R] {
        @transient lazy val data: D = createData

        override def flatMap(value: T, out: Collector[R]): Unit = {
          func(value, data) foreach out.collect
        }
      })
    }

    def mapLog(prefix: String, logIntervalMs: Long = 0L): DataStream[T] = {
      ds.map(new LogMap[T](prefix, logIntervalMs = logIntervalMs))(ds.dataType)
    }

    def addLogSink(): DataStreamSink[T] = {
      ds.addSink(new LogSink[T]).name("logsink")
    }

    /* json的DataStream[String] => DataStream[R] start */

    def json2DataStream[R: Manifest : TypeInformation]: DataStream[R] = {
      assert(ds.dataType eq BasicTypeInfo.STRING_TYPE_INFO)
      ds.asInstanceOf[DataStream[String]].map(JsonUtil.readValue[R](_))
    }

    def json2DataStream[R: TypeInformation](cls: Class[R]): DataStream[R] = {
      assert(ds.dataType eq BasicTypeInfo.STRING_TYPE_INFO)
      ds.asInstanceOf[DataStream[String]].map(JsonUtil.readValue[R](_, cls))
    }

    def json2DataStreamForType[R: Manifest : TypeInformation]: DataStream[R] = {
      assert(ds.dataType eq BasicTypeInfo.STRING_TYPE_INFO)
      ds.asInstanceOf[DataStream[String]].map(new RichMapFunction[String, R] {
        @transient var objReader: ObjectReader = _

        override def open(parameters: Configuration): Unit = objReader = JsonUtil.javaMapper.readerFor[R]

        override def map(value: String): R = objReader.readValue[R](value)
      })
    }

    def json2ScDataStream[R: Manifest : TypeInformation]: DataStream[R] = {
      assert(ds.dataType eq BasicTypeInfo.STRING_TYPE_INFO)
      ds.asInstanceOf[DataStream[String]].map(JsonUtil.readScValue[R](_))
    }

    def json2ScDataStream[R: TypeInformation](cls: Class[R]): DataStream[R] = {
      assert(ds.dataType eq BasicTypeInfo.STRING_TYPE_INFO)
      ds.asInstanceOf[DataStream[String]].map(JsonUtil.readScValue[R](_, cls))
    }

    def json2ScDataStreamForType[R: Manifest : TypeInformation]: DataStream[R] = {
      assert(ds.dataType eq BasicTypeInfo.STRING_TYPE_INFO)
      ds.asInstanceOf[DataStream[String]].map(new RichMapFunction[String, R] {
        @transient var objReader: ObjectReader = _

        override def open(parameters: Configuration): Unit = objReader = JsonUtil.scMapper.readerFor[R]

        override def map(value: String): R = objReader.readValue[R](value)
      })
    }

    /* json的DataStream[String] => DataStream[R] end */
  }

}

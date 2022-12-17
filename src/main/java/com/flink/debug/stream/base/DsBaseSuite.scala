package com.flink.debug.stream.base

import com.flink.base.FlinkBaseSuite
import com.flink.connector.localfile.LocalFileSourceFunction
import com.flink.debug.stream.base.DsBaseSuite.Log
import com.flink.serialization.{BinarySchema, JsonDataDeserializationSchema}
import com.flink.stream.func.DeserializeFunc
import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.json.JsonDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.scalatest.funsuite.AnyFunSuite

class DsBaseSuite extends AnyFunSuite {

  /**
   * 构造函数：[[org.apache.flink.runtime.deployment.TaskDeploymentDescriptor]]
   * [[org.apache.flink.streaming.runtime.tasks.StreamTask#invoke()]]
   */
  test("simple_etl_enableObjectReuse"){
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()
    //val tEnv = StreamTableEnvironment.create(env)

    val ds = env.addSource(new LocalFileSourceFunction[Array[Byte]]("D:\\IdeaWorkspace\\FlinkNote1.16\\files\\online_log.json",
      sleep = 1000, deserializer = new BinarySchema))

    val rstDs = ds.map(new DeserializeFunc(new JsonDataDeserializationSchema[Log](classOf[Log], true))).filter{ log =>
      log.bs == "hotel"
    }.map{ log =>
      (log.page_id, log.page_name, log.item_id, log.visit_time)
    }.map{ a =>
      a
    }

    rstDs.addSink{ data =>
      println(data)
    }.name("sink")

    env.execute("simple etl")
  }

  /**
   * 构造函数：[[org.apache.flink.runtime.deployment.TaskDeploymentDescriptor]]
   * [[org.apache.flink.streaming.runtime.tasks.StreamTask#invoke()]]
   */
  test("simple_etl_disableObjectReuse"){
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    //env.getConfig.enableObjectReuse()
    //val tEnv = StreamTableEnvironment.create(env)

    val ds = env.addSource(new LocalFileSourceFunction[Array[Byte]]("D:\\IdeaWorkspace\\FlinkNote1.16\\files\\online_log.json",
      sleep = 1000, deserializer = new BinarySchema))

    val rstDs = ds.map(new DeserializeFunc(new JsonDataDeserializationSchema[Log](classOf[Log], true))).filter{ log =>
      log.bs == "hotel"
    }.map{ log =>
      (log.page_id, log.page_name, log.item_id, log.visit_time)
    }.map{ a =>
      a
    }

    rstDs.addSink{ data =>
      println(data)
    }.name("sink")

    env.execute("simple etl")
  }

}

object DsBaseSuite {
  case class Log(
    dt: String,
    bs: String,
    report_time: String,
    browser_uniq_id: String,
    os_plant: String,
    page_id: String,
    page_name: String,
    page_param: String,
    item_id: java.lang.Long,
    item_type: java.lang.Integer,
    visit_time: String
  )
}
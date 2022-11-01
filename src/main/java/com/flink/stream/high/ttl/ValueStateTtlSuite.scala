package com.flink.stream.high.ttl

import java.sql.Timestamp
import java.util

import com.flink.base.FlinkBaseSuite
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/fault-tolerance/state/#state-time-to-live-ttl
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/datastream/fault-tolerance/state/#%E7%8A%B6%E6%80%81%E6%9C%89%E6%95%88%E6%9C%9F-ttl
 * 任何类型的 keyed state 都可以有 有效期 (TTL)。如果配置了 TTL 且状态值已过期，则会尽最大可能清除对应的值，这会在后面详述。
 * 所有状态类型都支持单元素的 TTL。 这意味着列表元素和映射元素将独立到期。这个挺好的，使用ValueState[Map[K, V]]和MapState[K, V]的区别是MapState中的每个K单独计算过期，当然这样额外存储的时间戳状态也更大。
 * 在使用状态 TTL 前，需要先构建一个StateTtlConfig 配置对象。 然后把配置传递到 state descriptor 中启用 TTL 功能
 *
 * 注意:
 *    1.状态上次的修改时间会和数据一起保存在 state backend 中，因此开启该特性会增加状态数据的存储。
 *      Heap state backend 会额外存储一个包括用户状态以及时间戳的 Java 对象，RocksDB state backend 会在每个状态值（list 或者 map 的每个元素）序列化后增加 8 个字节。
 *    2.暂时只支持基于 processing time 的 TTL。
 *    3.尝试从 checkpoint/savepoint 进行恢复时，TTL 的状态（是否开启）必须和之前保持一致，否则会遇到 “StateMigrationException”。
 *    4.TTL 的配置并不会保存在 checkpoint/savepoint 中，仅对当前 Job 有效。
 *    5.当前开启 TTL 的 map state 仅在用户值序列化器支持 null 的情况下，才支持用户值为 null。如果用户值序列化器不支持 null， 可以用 NullableSerializer 包装一层。
 *    6.启用 TTL 配置后，StateDescriptor 中的 defaultValue（已被标记 deprecated）将会失效。这个设计的目的是为了确保语义更加清晰，在此基础上，用户需要手动管理那些实际值为 null 或已过期的状态默认值。
 *
 */
class ValueStateTtlSuite extends FlinkBaseSuite {
  def parallelism: Int = 1

  /**
   * 这里测试ValueState：10 s不更新mapState, mapState整个就会置为null
   * mapState存的map作为一个整体，放入任何一个key，整个map都会更新过期时间
   *
   * 调试看下：
   * ValueState的实际实现类是：[[org.apache.flink.runtime.state.ttl.TtlValueState]]
   * TtlValueState对HeapValueState做了一个简单的包装，读取时会判断时间戳也可能更新时间戳当配置OnReadAndWrite时，更新时更新时间戳
   * TtlValueState实际储存的状态是:[[org.apache.flink.runtime.state.ttl.TtlValue]]
   * TtlValue就是额外多存了一个lastAccessTimestamp: Long，对于这个例子就是在HashMap之上多存了一个时间戳用于ttl，所以ttl过期作用于真个map
   *
   */
  test("ValueStateTtl"){
    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    text.keyBy(_ => 1)
      .flatMap(new RichFlatMapFunction[String, String]{
        @transient lazy val mapState: ValueState[util.HashMap[String, Integer]] = {
          val ttlConfig = StateTtlConfig
            .newBuilder(Time.seconds(10))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build
          val descriptor = new ValueStateDescriptor[util.HashMap[String, Integer]]("map", createTypeInformation[util.HashMap[String, Integer]])
          descriptor.enableTimeToLive(ttlConfig)
          getRuntimeContext.getState(descriptor)
        }

        def flatMap(value: String, out: Collector[String]): Unit = {
          val map = mapState.value() match {
            case null => new util.HashMap[String, Integer]
            case x => x
          }
          println(new Timestamp(System.currentTimeMillis()), "premap", map)

          map.put(value, map.getOrDefault(value, 0) + 1)

          println(new Timestamp(System.currentTimeMillis()), "map", map)

          mapState.update(map)
        }
      })
      .print()

  }

  /**
   * 启用 TTL 配置后，StateDescriptor 中的 defaultValue（已被标记 deprecated）将会失效。
   * 这个设计的目的是为了确保语义更加清晰，在此基础上，用户需要手动管理那些实际值为 null 或已过期的状态默认值。
   *
   * 不加descriptor.enableTimeToLive(ttlConfig), 默认值就生效，加上就不起作用。
   * 怪不得把带defaultValue的构造函数标记为Deprecated，让自己管理判断默认值。
   */
  test("ValueStateTtl_defaultValue"){
    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    text.keyBy(_ => 1)
      .flatMap(new RichFlatMapFunction[String, String]{
        @transient lazy val cntState: ValueState[Integer] = {
          val ttlConfig = StateTtlConfig
            .newBuilder(Time.seconds(10))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build
          val descriptor = new ValueStateDescriptor[Integer]("cnt", createTypeInformation[Integer], 10)
          descriptor.enableTimeToLive(ttlConfig)
          getRuntimeContext.getState(descriptor)
        }

        def flatMap(value: String, out: Collector[String]): Unit = {
          val cnt = Option(cntState.value()).getOrElse(0:Integer) + 1
          cntState.update(cnt)
          println(new Timestamp(System.currentTimeMillis()), "cnt", cnt)
        }
      })
      .print()
  }
}

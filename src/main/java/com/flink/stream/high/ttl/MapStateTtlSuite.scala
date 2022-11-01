package com.flink.stream.high.ttl

import java.sql.Timestamp

import com.flink.base.FlinkBaseSuite
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class MapStateTtlSuite extends FlinkBaseSuite {
  def parallelism: Int = 1

  /**
   * MapState的每个Entry的过期时间戳是独立维护的。而且MapState的pojo类型的value的状态恢复也是支持容错升级的。
   *
   * 调试看下：
   * MapState的实际实现类是：[[org.apache.flink.runtime.state.UserFacingMapState]]
   * UserFacingMapState仅仅包装实现返回空的map不返回null，实际实现的是[[org.apache.flink.runtime.state.ttl.TtlMapState]]
   * 看下TtlMapState(包装HeapMapState等)的方法：
   *    put: 把value额外加一个时间戳，实际存储的是[key, TtlValue[value]]
   *    entries: 在original.entries()返回的基础上，包装返回，判断时间戳
   *    iterator: 直接调用的entries().iterator()
   *    contains: 这也会判断过期，同时也是个读操作，配置OnReadAndWrite的话会更新时间戳
   *    putAll: 就是新建一个Map[K, TtlValue[V]]，在每个value加上时间戳放进去
   * 其实可以看到使用ttl还是有一定的性能损耗的
   */
  test("MapStateTtl"){
    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    text.keyBy(_ => 1)
      .flatMap(new RichFlatMapFunction[String, String] {
        @transient lazy val mapState: MapState[String,String] = {
          val ttlConfig = StateTtlConfig
            .newBuilder(Time.seconds(10))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build
          val descriptor = new MapStateDescriptor[String, String]("datas", createTypeInformation[String], createTypeInformation[String])
          descriptor.enableTimeToLive(ttlConfig)
          getRuntimeContext.getMapState(descriptor)
        }

        def flatMap(value: String, out: Collector[String]): Unit = {
          val time = new Timestamp(System.currentTimeMillis()).toString.substring(11, 19)
          mapState.put(value, time)

          val datas = mapState.iterator().asScala.map(x => (x.getKey, x.getValue) ).toList.sortBy(_._2)
          println(new Timestamp(System.currentTimeMillis()), datas)
        }
      })
  }


}

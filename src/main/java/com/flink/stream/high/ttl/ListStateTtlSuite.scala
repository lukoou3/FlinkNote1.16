package com.flink.stream.high.ttl

import java.sql.Timestamp

import com.flink.base.FlinkBaseSuite
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class ListStateTtlSuite extends FlinkBaseSuite {
  def parallelism: Int = 1

  /**
   * 这里测试ListState：10 s。每次add一个元素时，这个元素单独维护过期时间戳。每个元素时独立的
   *
   * 调试看下：
   *  ListState的实际实现类是：[[org.apache.flink.runtime.state.UserFacingListState]]
   *  UserFacingListState仅仅包装实现返回空的列表不返回null，实际实现的是[[org.apache.flink.runtime.state.ttl.TtlListState]]
   *  看下TtlListState(包装HeapListState等)的方法：
   *      add:在原始元素上加一个时间戳，实际存储的是[[org.apache.flink.runtime.state.ttl.TtlValue]]
   *      addAll:在每个元素上加一个时间戳
   *      update:新建List[TtlValue[T]]，把每个元素上加一个时间戳加入，调用原始original.update
   *      get:还是加了一层包装，获取Iterable[TtlValue[T]]后新建一个代理Iterable[T]返回增加时间戳的过滤
   *  可以看到ttl的实现就是原始每个元素额外增加一个时间戳。
   *  从代码实现实现也可以看出来就算是使用HeapState，更新之前元素只能update全部，直接修改元素的变量应该是不行的，而且还不知道rockdb怎么实现的，是不是每次都序列化了等。
   */
  test("ListStateTtl_add"){
    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    text.keyBy(_ => 1)
      .flatMap(new RichFlatMapFunction[String, String] {
        @transient lazy val datasState: ListState[String] = {
          val ttlConfig = StateTtlConfig
            .newBuilder(Time.seconds(10))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build
          val descriptor = new ListStateDescriptor[String]("datas", createTypeInformation[String])
          descriptor.enableTimeToLive(ttlConfig)
          getRuntimeContext.getListState(descriptor)
        }

        def flatMap(value: String, out: Collector[String]): Unit = {
          val time = new Timestamp(System.currentTimeMillis()).toString.substring(11, 19)
          datasState.add(s"$time:$value")
          val line = datasState.get().asScala.mkString(",")
          println(new Timestamp(System.currentTimeMillis()), line)
        }
      })
  }

  /**
   * 使用update相当于把列表所有元素的时间戳置为最新的。
   * 那么为啥我还要使用ListState，直接在ValueState中存list不行吗，一个重要的原因是ListState的pojo元素支持状态恢复升级，添加字段不会报错
   */
  test("ListStateTtl_update"){
    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    text.keyBy(_ => 1)
      .flatMap(new RichFlatMapFunction[String, String] {
        @transient lazy val datasState: ListState[String] = {
          val ttlConfig = StateTtlConfig
            .newBuilder(Time.seconds(10))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build
          val descriptor = new ListStateDescriptor[String]("datas", createTypeInformation[String])
          descriptor.enableTimeToLive(ttlConfig)
          getRuntimeContext.getListState(descriptor)
        }

        def flatMap(value: String, out: Collector[String]): Unit = {
          val time = new Timestamp(System.currentTimeMillis()).toString.substring(11, 19)
          val datas = datasState.get().asScala.toBuffer
          datas += s"$time:$value"
          datasState.update(datas.asJava)
          val line = datas.mkString(",")
          println(new Timestamp(System.currentTimeMillis()), line)
        }
      })
  }

}

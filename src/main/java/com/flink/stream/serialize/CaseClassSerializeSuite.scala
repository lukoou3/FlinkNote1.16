package com.flink.stream.serialize

import CaseClassSerializeSuite._
import com.flink.base.FlinkBaseSuite
import com.flink.stream.source.UniqueIdSequenceSource
import org.apache.flink.streaming.api.scala._

/**
 * case和元组一样，都可以通过元组的方式序列化
 * 元组本身data不能为null,元组的属性值类型为元组时这个属性也不能为null;元组的其它类型的属性值可以为null
 * 觉得使用元组方便，还想用null，可以使用option/some/none，这个测试是可以的
 * 实际上官网也说了可以使用scala中的这几个特殊类型,使用起来还是挺方便的。
 * Special Types：You can use special types, including Scala’s Either, Option, and Try. The Java API has its own custom implementation of Either. Similarly to Scala’s Either, it represents a value of two possible types, Left or Right. Either can be useful for error handling or operators that need to output two different types of records.
 * 官网数据类型和序列化的说明：
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/fault-tolerance/serialization/types_serialization/
 *
 * pojo类型作为元组的属性时，值为null返回时没有报错。任何类型都不能把整个对象返回null。
 */
class CaseClassSerializeSuite extends FlinkBaseSuite {
  override def parallelism: Int = 2

  /**
   * [[org.apache.flink.api.scala.typeutils.CaseClassSerializer]]
   * [[org.apache.flink.api.scala.typeutils.ScalaCaseClassSerializer]]:
   * 看代码：
   *    copy属性为null没问题
   *    serialize就会空指针异常，Integer竟然用的是int value的序列化，没判断空。
   *    string null 没问题，写入了字符序列的长度
   *    Integer有问题string竟然没问题。flink就没认真考虑scala case class的序列化情况
   *    Option[Int]属性没问题，因为做了null值判断[[org.apache.flink.api.scala.typeutils.OptionSerializer]]
   *
   *  [[org.apache.flink.api.java.typeutils.runtime.PojoSerializer]]：
   *  serialize方法中就对属性的null做了判断，null值有flag
   *
   * 每个属性的序列化：
   * [[org.apache.flink.api.common.typeinfo.BasicTypeInfo]]
   * case class的序列化就是各个属性fieldSerializers(i)依次调用serialize和deserialize，没有null值的校验
   * string属性为null为啥可以：
   *    [[org.apache.flink.api.common.typeutils.base.StringSerializer]]
   *    StringValue.writeString(record, target),StringValue.readString(source) 会写入和读取字符串的长度
   */
  test("CaseClass") {
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))
    ds.map(_.toInt)
      .map { id => ScalaCase(id, s"name:$id", id % 100)  }
      .rebalance
      .addSink{data =>
        println(data)
      }

    env.execute("CaseClassSerialize")
  }

  test("CaseClassNullField") {
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))
    ds.map(_.toInt)
      .map { id =>
        // 可以
        //ScalaCase(id, null, id % 100)
        // 不行：org.apache.flink.api.common.typeutils.base.IntSerializer
        // ScalaCase(id, null, null)
        ScalaCase(id, null, id % 100)
      }
      .rebalance
      .addSink{data =>
        println(data)
      }

    env.execute("CaseClassSerialize")
  }

}

object CaseClassSerializeSuite {

  case class ScalaCase(
    var id: Int = 0,
    var name: String = null,
    var age: Integer = null
  ){
    //println(id)
  }

}
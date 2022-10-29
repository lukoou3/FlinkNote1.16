package com.flink.stream.serialize

import com.flink.base.FlinkBaseSuite
import com.flink.stream.source.UniqueIdSequenceSource
import org.apache.flink.streaming.api.scala._

import scala.beans.BeanProperty
import PojoClassSerializeSuite._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.PojoTestUtils

/**
 * [[org.apache.flink.api.java.typeutils.runtime.PojoSerializer]]
 * 写入和读取属性会写入读取null标识, pojo没有属性null序列化的问题
 *
 */
class PojoClassSerializeSuite extends FlinkBaseSuite {
  override def parallelism: Int = 3

  test("ScalaPojoClass"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))
    ds.map(_.toInt)
      .map { id =>
        val data = new ScalaPojo
        data.id = id
        data.name = s"name:$id"
        data.age = id % 100
        data
      }
      .rebalance
      .addSink{data =>
        println(data)
      }

    env.execute("ScalaPojoClassSerialize")
  }

  test("ScalaPojoClassNullField"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))
    ds.map(_.toInt)
      .map { id =>
        val data = new ScalaPojo
        data
      }
      .rebalance
      .addSink{data =>
        println(data)
      }

    env.execute("ScalaPojoClassNullFieldSerialize")
  }

}

object PojoClassSerializeSuite{

  def main1(args: Array[String]): Unit = {
    PojoTestUtils.assertSerializedAsPojo(classOf[ScalaPojo])
    PojoTestUtils.assertSerializedAsPojoWithoutKryo(classOf[ScalaPojo])

    PojoTestUtils.assertSerializedAsPojo(classOf[ScalaPojo1])
    PojoTestUtils.assertSerializedAsPojoWithoutKryo(classOf[ScalaPojo1])

    println(createTypeInformation[ScalaPojo1])
    println(TypeInformation.of(classOf[ScalaPojo1]))

    println("success")
  }

  class ScalaPojo extends Serializable {
    @BeanProperty var id: Int = _
    @BeanProperty var name: String = _
    @BeanProperty var age: Integer = _

    override def toString = s"ScalaPojo(id=$id, name=$name, age=$age)"
  }

  // 生成的属性是private的，但flink还是识别成pojo
  class ScalaPojo1 extends Serializable {
    var id: Int = _
    var name: String = _
    var age: Integer = _

    override def toString = s"ScalaPojo1(id=$id, name=$name, age=$age)"
  }

}

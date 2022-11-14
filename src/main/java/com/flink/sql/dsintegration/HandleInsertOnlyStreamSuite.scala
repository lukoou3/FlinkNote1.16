package com.flink.sql.dsintegration

import java.time.Instant

import com.flink.base.FlinkBaseSuite
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import HandleInsertOnlyStreamSuite._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter

/**
 * 之前的直接通过传入可变参数重命名列名和定义处理/事件时间的方法被标记废弃了
 * 这些方法都被标记废弃了，之后的版本可能会删除：fromDataStream(stream, $"myLong", $"myString")/toAppendStream/toRetractStream
 * 新版本使用这几个方法：fromDataStream/toDataStream, fromChangelogStream/toChangelogStream
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#examples-for-fromdatastream
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#legacy-conversion
 *
 * InsertOnly的Stream和table的转化有下面几个函数可以使用：
 *    fromDataStream(DataStream):将insert-only的流解释为表。默认情况下，不会传播事件时间和水印。
 *    fromDataStream(DataStream, Schema):将insert-only的流解释为表。可选模式允许丰富列数据类型并添加时间属性、水印策略、其他计算列或主键。
 *    createTemporaryView(String, DataStream):注册一个临时表名。它是createTemporaryView(String, fromDataStream(DataStream))的快捷方式。
 *    createTemporaryView(String, DataStream, Schema):它是createTemporaryView(String, fromDataStream(DataStream, Schema))的快捷方式。
 *    toDataStream(Table):将表转换为insert-only的流。默认流record类型为Row。
 *    toDataStream(Table, AbstractDataType):将表转换为insert-only的流。指定类型。
 *    toDataStream(Table, Class):它是toDataStream(Table, DataTypes.of(Class))的快捷方式。
 */
class HandleInsertOnlyStreamSuite extends FlinkBaseSuite{
  def parallelism: Int = 1

  /**
   * scala类的typeinfo要使用scala隐式参数推导，TypeInformation.of(classOf[People])这种方式并不能解析的到
   * 打印一下得到的TypeInformation：
   *    TypeInformation.of(classOf[People]) = GenericType[com.flink.sql.dsintegration.HandleInsertOnlyStreamSuite.People]
   *    implicitly[TypeInformation[People]] = People(id: Long, name: String, age: Integer, score: Double, event_time: Instant)
   */
  test("scalaTypeInformation"){
    execute = false
    val typeInformation1 = TypeInformation.of(classOf[People])
    val typeInformation2 = implicitly[TypeInformation[People]]
    println(typeInformation1)
    println(typeInformation2)
  }

  test("ds转table_fromDataStream") {
    val datas = Seq(
      People(1, "aaa", 20, 90.2, Instant.ofEpochMilli(1000)),
      People(2, "bb", 22, 90.2, Instant.ofEpochMilli(1001)),
      People(3, "cc", 21, 90.2, Instant.ofEpochMilli(1002))
    )

    // scala类的typeinfo要使用scala隐式参数推导，TypeInformation.of(classOf[People])这种方式并不能解析的到
    //val ds = env.fromCollection(datas)(TypeInformation.of(classOf[People]))
    val ds = env.fromCollection(datas)

    //val inputTypeInfo = TypeInformation.of(classOf[People])
    val inputTypeInfo = implicitly[TypeInformation[People]]
    val inputDataType = TypeInfoDataTypeConverter.toDataType(tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory,
      inputTypeInfo)
    println(inputDataType)

    /**
     * new DataStructureConverterWrapper(DataStructureConverters.getConverter(producedDataType))
     *    org.apache.flink.table.runtime.connector.source.DataStructureConverterWrapper:
     *        structureConverter.toInternalOrNull(externalStructure)
     *    org.apache.flink.table.runtime.connector.sink.DataStructureConverterWrapper:
     *        structureConverter.toExternalOrNull(internalStructure)
     *
     * 自动推断所有的physical columns
     * ds的.TypeInfo和sql的DataType的对应关系在：[[org.apache.flink.table.types.utils.TypeInfoDataTypeConverter.conversionMap]]
     * Instant对应TIMESTAMP_LTZ
     * sql内部的类型对应关系可以看：LogicalTypeUtils.toInternalConversionClass
     * sql类型的实际转化例子可以看官方实现的connector的读写，可以json转化的实现为例查看：JsonToRowDataConverters和RowDataToJsonConverters
     */
    var table = tEnv.fromDataStream(ds)
    table.printSchema()

    /**
     * 自动推断所有的physical columns
     * 同时添加计算列
     */
    table = tEnv.fromDataStream(ds, Schema.newBuilder()
      .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
      .columnByExpression("proc_time", "PROCTIME()")
      .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
      .build()
    )
    table.printSchema()

    //table.execute().print()
    table.toDataStream.addSink {
      row => println(row)
    }
  }

}

object HandleInsertOnlyStreamSuite{
  case class People(id: Long, name: String, age: Int, score: Double, event_time: Instant)
}

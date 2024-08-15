package com.flink.sql.dsintegration

import java.time.Instant

import com.flink.base.FlinkBaseSuite
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import HandleInsertOnlyStreamSuite._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{DataTypes, Schema}
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.data.conversion.DataStructureConverters
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.runtime.typeutils.{ExternalTypeInfo, InternalTypeInfo}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter
import org.apache.flink.types.Row

/**
 * 之前的直接通过传入可变参数重命名列名和定义处理/事件时间的方法被标记废弃了
 * 这些方法都被标记废弃了，之后的版本可能会删除：fromDataStream(stream, $"myLong", $"myString")/toAppendStream/toRetractStream
 * 新版本使用这几个方法：fromDataStream/toDataStream, fromChangelogStream/toChangelogStream
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#examples-for-fromdatastream
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#examples-for-fromchangelogstream
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

  test("ds转table_fromDataStream_DataStream[Row]") {
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")

    val rowDs: DataStream[Row] = table.toDataStream
    println(rowDs.dataType)

    /**
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * internalRecord = converter.toInternal(externalRecord)
     * [[org.apache.flink.table.data.conversion.RowRowConverter]]
     */
    val rstTable = tEnv.fromDataStream(rowDs)


    rstTable.toDataStream.addSink { row =>
      println(row)
    }
  }

  test("ds转table_fromDataStream_DataStream[RowData]") {
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")

    val rowDataDataType: DataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs: DataStream[RowData] = table.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    val dataType = rowDataDs.dataType
    println(rowDataDs.dataType)

    /**
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * internalRecord = converter.toInternal(externalRecord)
     * [[org.apache.flink.table.data.conversion.IdentityConverter]]
     */
    val rstTable = tEnv.fromDataStream(rowDataDs)

    rstTable.toDataStream.addSink { row =>
      println(row)
    }
  }

  test("ds转table_fromDataStream_DataStream[RowData]2"){
    val names = Array("莫南","璇音","青丝","流沙")

    val rowDataDs: DataStream[RowData] = env.fromSequence(1, 10000).map{ i =>
      if(i % 5 > 0){
        val name = names((i % 4).toInt)
        val age = java.lang.Integer.valueOf((18 + i % 100).toInt)
        val cnt = java.lang.Long.valueOf(18 + i % 100)
        val row: RowData = GenericRowData.of(StringData.fromString(name), age, cnt)
        row
      }else{
        GenericRowData.of(null, null, null)
      }
    }(ExternalTypeInfo.of(
      DataTypes.ROW(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("cnt", DataTypes.BIGINT())
      ).bridgedTo(classOf[RowData])
    ))

    println((ExternalTypeInfo.of(
      DataTypes.ROW(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("cnt", DataTypes.BIGINT())
      ).bridgedTo(classOf[RowData])
    )))
    val typeInformation: TypeInformation[RowData] = ExternalTypeInfo.of(
      tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory
        .createDataType("ROW<name string, age int, cnt bigint>").bridgedTo(classOf[RowData]))
    println(typeInformation)

    /**
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * internalRecord = converter.toInternal(externalRecord)
     * [[org.apache.flink.table.data.conversion.IdentityConverter]]
     */
    val rstTable = tEnv.fromDataStream(rowDataDs)


    val rowDataDataType: DataType = rstTable.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs2: DataStream[RowData] = rstTable.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    println(rowDataDs2.dataType)

    rowDataDs2.addSink{ row =>
      println(row)
    }
  }

  test("ds转table_fromDataStream_DataStream[RowData]3"){
    val names = Array("莫南","璇音","青丝","流沙")

    val rowDataDs: DataStream[RowData] = env.fromSequence(1, 10000).map{ i =>
      Thread.sleep(500)
      if(i % 5 > 0){
        val name = names((i % 4).toInt)
        val age = java.lang.Integer.valueOf((18 + i % 100).toInt)
        val cnt = java.lang.Long.valueOf(18 + i % 100)
        val row: RowData = GenericRowData.of(StringData.fromString(name), age, cnt)
        row
      }else{
        GenericRowData.of(null, null, null)
      }
    }(ExternalTypeInfo.of(
      tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory
        .createDataType("ROW<name string, age int, cnt bigint>").bridgedTo(classOf[RowData]))
    )

    /**
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * internalRecord = converter.toInternal(externalRecord)
     * [[org.apache.flink.table.data.conversion.IdentityConverter]]
     */
    val rstTable = tEnv.fromDataStream(rowDataDs)


    val rowDataDataType: DataType = rstTable.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs2: DataStream[RowData] = rstTable.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    println(rowDataDs2.dataType)

    rowDataDs2.addSink{ row =>
      println(row)
    }
  }

  test("ds转table_fromDataStream_DataStream[RowData]4"){
    val names = Array("莫南","璇音","青丝","流沙")

    val rowDataDs: DataStream[RowData] = env.fromSequence(1, 10000).map{ i =>
      if(i % 5 > 0){
        val name = names((i % 4).toInt)
        val age = java.lang.Integer.valueOf((18 + i % 100).toInt)
        val cnt = java.lang.Long.valueOf(18 + i % 100)
        val row: RowData = GenericRowData.of(StringData.fromString(name), age, cnt)
        row
      }else{
        GenericRowData.of(null, null, null)
      }
    }(InternalTypeInfo.of[RowData](
      tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory
        .createDataType("ROW<name string, age int, cnt bigint>").getLogicalType)
    )


    /**
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * internalRecord = converter.toInternal(externalRecord)
     * [[org.apache.flink.table.data.conversion.IdentityConverter]]
     */
    val rstTable = tEnv.fromDataStream(rowDataDs)


    val rowDataDataType: DataType = rstTable.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs2: DataStream[RowData] = rstTable.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    val tape1 = InternalTypeInfo.of[RowData](
      tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory
        .createDataType("ROW<name string, age int, cnt bigint>").getLogicalType)
    val tape2 = rowDataDs2.dataType
    println(tape1)
    println(tape2)
    println()

    rowDataDs2.addSink{ row =>
      println(row)
    }
  }

  test("table转ds_toDataStream_DataStream[Row]"){
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      data row<name string, age int>,
      names array<string>,
      datas array<row<name string, age int>>,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")

    val rowDs: DataStream[Row] = table.toDataStream
    println(rowDs.dataType)

    rowDs.filter(x => true).addSink { row =>
      println(row)
    }
  }

  test("table转ds_toDataStream_DataStream[RowData]"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      data row<name string, age int>,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp(3),
      log_time_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.log_time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")

    /**
     * 就是直接调用的： [[StreamTableEnvironment.toDataStream(Table)]]
     * 实际实现功能的是：[[org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl.toDataStream(Table)]]
     * 然后调用的代码是：
     * // include all columns of the query (incl. metadata and computed columns)
     * val sourceType = table.getResolvedSchema.toSourceRowDataType
     * toDataStream(table, sourceType)
     * 没有bridged到RowData，所以生成的就是Row类型
     */
    val rowDs: DataStream[Row] = table.toDataStream
    println(rowDs.dataType)


    /**
     * 转RowData，用新的方法需要显示绑定使用内部类型
     * 使用toAppendStream[RowData]老的api则可以直接转为RowData。
     */
    val rowDataDataType: DataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs: DataStream[RowData] = table.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    println(rowDataDs.dataType)

    rowDataDs.addSink { row =>
      println(row)
    }
  }

  test("table转ds_toDataStream_DataStream[CaseClass]"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      data row<name string, age int>,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp(3),
      log_time_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
       -- 'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.log_time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select name,age,cnt,data,datas from tmp_tb1")

    /**
     * 把RowData转成外部类型
     */
    val dataType = DataTypes.of(classOf[TableCaseData]).toDataType(tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory)
    println(dataType)
    val structureConverter = DataStructureConverters.getConverter(dataType)
    //new DataStructureConverterWrapper(DataStructureConverters.getConverter(dataType))

    /**
     * table转ds会加一个filter校验过滤非空属性，默认data中的非空属性table这行实际的列值为null时会抛出异常
     * 设置'table.exec.sink.not-null-enforcer'='drop'后，会直接把这一行给直接过滤掉，肯定不能设置这个
     * 这个是运行时报错的，要是实际就没null，转的data中使用原生类型也是不会报错的。不是编译阶段报错
     * org.apache.flink.table.runtime.operators.sink.SinkNotNullEnforcer
     *
     * 实际相当于调用：toDataStream(table, DataTypes.of(targetClass))
     */
    val dataDs = table.toDataStream(classOf[TableCaseData])
    println(dataDs.dataType)
    //dataDs.print()
    dataDs.addSink { row =>
      println(row)
    }
  }

}

object HandleInsertOnlyStreamSuite{
  case class People(id: Long, name: String, age: Int, score: Double, event_time: Instant)

  case class TableCaseSubData(name: String, age: Integer)

  //case class TableCaseData(name: String, age: Integer, cnt: java.lang.Long, data: TableCaseSubData, datas:java.util.List[TableCaseSubData])
  //case class TableCaseData(name: String, age: Integer, cnt: java.lang.Long, data: TableCaseSubData, datas:Array[TableCaseSubData])

  case class TableCaseData(name: String, age: Int, cnt: java.lang.Long, data: TableCaseSubData, datas: Array[TableCaseSubData]){
    println(111)
  }
}

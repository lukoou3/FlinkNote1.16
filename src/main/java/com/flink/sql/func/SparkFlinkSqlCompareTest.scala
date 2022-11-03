package com.flink.sql.func

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.types.Row

/**
 * 这是之前1.12调试查看的
 */
object SparkFlinkSqlCompareTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    //getLocalTimeZone默认是本地的时区, from_unixtime和unix_timestamp都会感知时区
    //tEnv.getConfig().setLocalTimeZone()

    // 注册函数
    tEnv.createTemporarySystemFunction("get_json_object", classOf[GetJsonObject])
    tEnv.createTemporarySystemFunction("split", classOf[Split])
    // 经过测试可以覆盖默认的函数
    //tEnv.createTemporarySystemFunction("substr", classOf[Substr])

    val onlineLog: DataStream[Log] = env.addSource(new FileSource("D:\\ideaProjects\\FlinkNote1.16\\files\\online_log.json")).map(new RichMapFunction[String, Log] {

      var mapper: ObjectMapper with ScalaObjectMapper = _

      override def open(parameters: Configuration): Unit = {
        mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
      }

      override def map(value: String): Log = {
        mapper.readValue[Log](value)
      }
    })

    tEnv.createTemporaryView("tmp_tb", onlineLog)

    /**
     * [[org.apache.flink.table.planner.codegen.calls.FunctionGenerator]]
     * [[org.apache.flink.table.planner.codegen.calls.BuiltInMethods]]
     *
     * [[org.apache.flink.table.planner.codegen.calls.StringCallGen]]:
     * [[org.apache.flink.table.runtime.functions.SqlFunctionUtils.regExp]]
     * [[org.apache.flink.table.runtime.functions.SqlFunctionUtils.regexpExtract]]
     *
     * 匹配sql操作符的逻辑: [[org.apache.flink.table.planner.codegen.ExprCodeGenerator#generateCallExpression]]
     * 匹配sql函数的逻辑generateCallExpression函数中:
     *    1.现在StringCallGen中查找: StringCallGen.generateCallExpression(ctx, call.getOperator, operands, resultType)
     *    2.然后在FunctionGenerator中查找: FunctionGenerator.getCallGenerator(sqlOperator)
     *    3.总的代码就是 StringCallGen.generateCallExpression(sqlOperator).getOrElse(FunctionGenerator.getCallGenerator(sqlOperator))
     *    4.所有的函数在StringCallGen和BuiltInMethods中查找就对了
     *
     * 靠flink sql很坑啊:
     *  1.没有sql_a regexp sql_b语法, 提供了一个regexp(sql_a, sql_b)函数
     *  2.数组访问时下标是从1开始的, 这点和hive/sparksql区别很大啊!!!
     *    https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/table/functions/systemFunctions.html#collection-functions
     *    array ‘[’ integer ‘]’ => Returns the element at position integer in array. The index starts from 1.
     */
    val sql =
      """
    select
        page_id,
        item_type,
        item_id,
        case
            when page_id = 'HotelRN_Detail' and trim(page_param) like '{%'
            then cast(regexp_extract(get_json_object(page_param, '$.page_param'), '([0-9]+)_[^_]*_[0-9]{4}-[0-9]{2}-[0-9]{2}', 1) as bigint)
            when page_id = 'HotelRN_Detail'
            then cast(regexp_extract(page_param, '([0-9]+)_[^_]*_[0-9]{4}-[0-9]{2}-[0-9]{2}', 1) as bigint)
            when regexp(page_name, 'hotel.m.jd.com/detail/[0-9]+')
            then cast(regexp_extract(page_name, 'hotel.m.jd.com/detail/([0-9]+)', 1) as bigint)
            when page_name like '%m3-ptrip.jd.com/productDetail%'
            then cast(regexp_extract(page_param, 'skuId=([0-9]+)', 1) as bigint)
            when page_id = 'ThemePark_Scenic' and instr(os_plant, '-M') > 0
            then cast(split(get_json_object('{"page_param": "-999_-888"}', '$.page_param'), '_')[1 + 1] as bigint)
            when page_id = 'ThemePark_Scenic' and trim(page_param) like '{%'
            then cast(split(get_json_object(page_param, '$.page_param'), '_')[1 + 1] as bigint)
            else cast(split(page_param, '_')[1 + 1] as bigint)
        end id,
        visit_time,
        from_unixtime(unix_timestamp(report_time,'yyyyMMdd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') visit_time2,
        -- from_unixtime和unix_timestamp都会感知时区
        from_unixtime(60*60, 'yyyy-MM-dd HH:mm:ss') tm,
        substr(dt, 1) s1,
        substr(dt, 1, 7) s2,
        get_json_object('{"a": -1}', '$.a') f1,
        get_json_object('{"a": 1, "b": 2}', '$.a', '$.b') f2,
        get_json_object(null, '$.a', '$.b') f3,
        get_json_object('{"a": 1, "b": 2}', '$.a', '$.b').f1 f4
    from tmp_tb
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    //rstTable.execute().print()

    import scala.collection.JavaConverters._
    val names = rstTable.getSchema.getTableColumns.asScala.map(_.getName).zipWithIndex

    println("1" * 20)
    val features = new Array[SerializerFeature](0)

    /**
     * rstTable.toAppendStream[Row]会在driver端调用一次自定义函数的open和eval和close
     * 注意不是所有的自定义函数会在driver端调, 只有输入是固定值时才会, 比如split(get_json_object('{"page_param": "aa_11"}', '$.page_param'), '_')[1]
     * 返回时固定的就没必要每条记录都调用一次, 每个subtask就不会再调用了。其它的情况不会在driver端调, 这只是特殊情况。
     * [[org.apache.flink.table.planner.codegen.ExpressionReducer.reduce()]]
     *
     * table.toAppendStream[T]通过隐士转换调用的:
     * tEnv.toAppendStream(table)
     * [[org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl.toAppendStream(Table, TypeInformation)]]
     * [[org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl#toDataStream]]
     * toAppendStream时虽然只添加了最后的streamTransformation输出是row, 但是其中包含了输入: table转stream => 依赖sql语句生成代码 => 依赖stream转table
     * 解析transformation时会解析依赖的父输入, 如果父transformation没解析会解析transformation添加Node
     *
     * -- sql代码生成查看
     * SourceConversion、StreamExecCalc、SinkConversion都是在[[org.apache.flink.table.planner.codegen.OperatorCodeGenerator.generateOneInputStreamOperator]]方法中生成的
     * LookupFunction(dim维度表)是在[[org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator.generateSyncLookupFunction]]方法中生成的
     * 生成的分别是GeneratedOperator和GeneratedFunction
     * 要看生成代码的逻辑就去OperatorCodeGenerator和LookupJoinCodeGenerator中看，这个包下还有其他代码生成的逻辑，之后可以看看，org.apache.flink.table.planner.codegen
     *
     * -- 以下为sql自动生成代码分析:
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * [[org.apache.flink.table.runtime.operators.TableStreamOperator]]
     * [[org.apache.flink.streaming.api.operators.OneInputStreamOperator]]
     *
     * SourceConversion:
     *    现在的不是代码生成的用的是InputConversionOperator
     *    [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     *    这是之前的：extends AbstractProcessStreamOperator implements OneInputStreamOperator
     *    用户使用类型转换成sql内部类型
     *    [[org.apache.flink.table.data.util.DataFormatConverters.CaseClassConverter#toInternal]]
     *    [[org.apache.flink.table.data.util.DataFormatConverters.CaseClassConverter#toInternalImpl]]
     *
     * StreamExecCalc:
     *    extends TableStreamOperator implements OneInputStreamOperator
     *    这是之前的：extends AbstractProcessStreamOperator implements OneInputStreamOperator
     *    class内部使用单个out, 全部生成代码在processElement方法中, sql的性能损失在stream -> Table -> stream 两次类型转换的每次新建对象
     *    当然数据转换的性能损失只在我们注册临时表时有, 使用sql connector时还得看源码, 可能不用转换。这点和spark sql一样
     *    org.apache.flink.table.data.BoxedWrapperRowData out = new org.apache.flink.table.data.BoxedWrapperRowData(12)
     *    output.collect(outElement.replace(out))
     *
     * SinkConversion:
     *    extends TableStreamOperator implements OneInputStreamOperator
     *    sql内部类型转换成用户使用类型
     *    [[org.apache.flink.table.data.util.DataFormatConverters.RowConverter#toExternal]]
     *    [[org.apache.flink.table.data.util.DataFormatConverters.RowConverter#toExternalImpl()]]
     *
     */
    rstTable.toAppendStream[Row].map{ row =>
      val map = names.map{ case (name, i) =>
        val v = row.getField(i)
        (name, if(v == null) null else v.toString)
      }.toMap.asJava
      JSON.toJSONString(map, features: _*)
    }.print()

    println("2" * 20)
    env.execute()
    println("3" * 20)
  }

  // define function logic
  class GetJsonObject extends ScalarFunction {
    def eval(s: String, f: String): String = {
      if(s == null){
        null
      }else{
        try {
          JSON.parseObject(s).getString(f.substring(2))
        } catch {
          case e: Exception =>
            println(s)
            //throw e
            null
        }
      }
    }

    // 定义嵌套数据类型
    @DataTypeHint("ROW<f1 STRING, f2 STRING>")
    def eval(s: String, f1: String, f2: String): Row = {
      if(s == null){
        null
      }else{
        try {
          val json = JSON.parseObject(s)
          Row.of(json.getString(f1.substring(2)), json.getString(f2.substring(2)))
        } catch {
          case e: Exception =>
            println(s)
            //throw e
            null
        }
      }
    }
  }


  class Split extends ScalarFunction {

    override def open(context: FunctionContext): Unit = {
      println("Split open:" + Thread.currentThread())
    }

    override def close(): Unit = {
      println("Split close:" + Thread.currentThread())
    }

    def eval(s: String, f: String): Array[String] = {
      if(s == null){
        null
      }else{
        s.split(f)
        // flink数组类型值为null或者索引越界都会返回null, 还好这点和spark一样
        //null
        //Array.empty
      }
    }
  }

  // 测试覆盖内置的函数
  class Substr extends ScalarFunction {
    override def open(context: FunctionContext): Unit = {
      println("Substr open:" + Thread.currentThread())
    }

    override def close(): Unit = {
      println("Substr close:" + Thread.currentThread())
    }

    def eval(s: String, pos: Int): String = {
      if(s == null){
        null
      }else{
        s.slice(pos - 1, s.length) + "_substr1"
      }
    }
    def eval(s: String, pos: Int, size: Int): String = {
      if(s == null){
        null
      }else{
        s.slice(pos - 1, pos - 1 + size) + "_substr2"
      }
    }
  }

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


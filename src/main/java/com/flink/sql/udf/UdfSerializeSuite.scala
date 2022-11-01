package com.flink.sql.udf

import java.util.Optional

import com.flink.base.FlinkBaseSuite
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import UdfSerializeSuite._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{CallContext, TypeInference, TypeStrategy}

class UdfSerializeSuite extends FlinkBaseSuite{
  def parallelism: Int = 4

  /**
   * 4个task，只是每个task有一个udf，udf_ser(name, 'a')和udf_ser(name, 'b')没区分开
   * 它这函数的序列化真傻屌，单个task的2个udf_ser序列化后还是同一个对象，不是2个
   * getTypeInference中修改udf的属性可以实现2个不同的对象
   * 都1.16了还是这样，难道被人都没遇到这个问题反馈?
   */
  test("UdfSerializeFunc"){
    tEnv.createTemporarySystemFunction("udf_ser", classOf[UdfSerializeFunc])

    var sql = """
    CREATE TEMPORARY TABLE heros (
      `name` STRING,
      `power` STRING,
      `age` INT
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.power.expression' = '#{superhero.power}',
      'fields.power.null-rate' = '0.05',
      'rows-per-second' = '1',
      'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        udf_ser(name, 'a') name1,
        udf_ser(name, 'b') name2
    from heros
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }

  /**
   * 修改ScalarFunction的属性，能使之序列化后是不同的对象
   */
  test("UdfSerializeFunc2"){
    tEnv.createTemporarySystemFunction("udf_ser", classOf[UdfSerializeFunc2])

    var sql = """
    CREATE TEMPORARY TABLE heros (
      `name` STRING,
      `power` STRING,
      `age` INT
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.power.expression' = '#{superhero.power}',
      'fields.power.null-rate' = '0.05',
      'rows-per-second' = '1',
      'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        udf_ser(name, 'a') name1,
        udf_ser(name, 'b') name2
    from heros
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }

}

object UdfSerializeSuite {

  class UdfSerializeFunc extends ScalarFunction {
    @transient var cache: String = null

    override def open(context: FunctionContext): Unit = {
      val udf = this
      println("open", Thread.currentThread(), udf.hashCode())
    }

    def eval(a: String, b: String): String = {
      if(cache == null){
        println("cache_null")
        cache = b
      }
      cache
    }

  }

  class UdfSerializeFunc2 extends ScalarFunction {
    var cache: String = null

    override def open(context: FunctionContext): Unit = {
      val udf = this
      println("open", Thread.currentThread(), udf.hashCode())
    }

    def eval(a: String, b: String): String = {
      cache
    }

    override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
      TypeInference.newBuilder()
        .outputTypeStrategy(new TypeStrategy {
          override def inferType(callContext: CallContext): Optional[DataType] = {
            val argumentDataTypes = callContext.getArgumentDataTypes
            if (argumentDataTypes.size() != 2) {
              throw callContext.newValidationError(s"input to function explode should be array or row type, not $argumentDataTypes")
            }
            cache = callContext.getArgumentValue(1, classOf[String]).get()
            Optional.of(DataTypes.STRING())
          }
        })
        .build()
    }

  }

}
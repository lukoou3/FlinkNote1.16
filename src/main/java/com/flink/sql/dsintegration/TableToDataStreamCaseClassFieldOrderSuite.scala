package com.flink.sql.dsintegration

import com.flink.base.FlinkBaseSuite

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._

import TableToDataStreamCaseClassFieldOrderSuite._

class TableToDataStreamCaseClassFieldOrderSuite extends FlinkBaseSuite {
  override def parallelism: Int = 1

  test("test"){
    val sql = """
    CREATE TABLE tmp_tb (
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

    val table = tEnv.sqlQuery("select name,age,cnt from tmp_tb")

    val ds1 = table.toDataStream(classOf[CaseClassData1])
    val ds2 = table.toDataStream(classOf[CaseClassData2])
    val ds3 = table.toDataStream(classOf[CaseClassData3])
    val ds4 = table.toDataStream(classOf[CaseClassData4])
    ds1.addSink{data =>
      println("ds1", data)
    }
    ds2.addSink{data => println("ds2", data) }
    ds3.addSink{data =>
      println("ds3", data)
    }
    ds4.addSink{data =>
      println("ds4", data)
    }
  }

}

object TableToDataStreamCaseClassFieldOrderSuite{
  case class CaseClassData1(name: String, age: Int, cnt: java.lang.Long)
  //case class CaseClassData2(name: String, cnt: Int, age: java.lang.Long)
  case class CaseClassData2(name: String, cnt: java.lang.Long, age: Int)
  case class CaseClassData3(name2: String, age2: Int, cnt2: java.lang.Long)
  case class CaseClassData4(age: Int, cnt: java.lang.Long, name: String)
}
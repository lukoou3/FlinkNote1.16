package com.flink.stream.high.timer

object ScalaObjectIntTest {

  def value2(): java.lang.Integer ={
    null
  }


  def main(args: Array[String]): Unit = {
    var a:Int = 1
    var b:java.lang.Integer = 1

    /**
     * 第一个会返回int的默认值，后三个报空指针异常
     * a = BoxesRunTime.unboxToInt(ObjectIntTest.value());
     * a = scala.Predef$.MODULE$.Integer2int(ObjectIntTest.value2());
     * a = scala.Predef$.MODULE$.Integer2int(b);
     * a = scala.Predef$.MODULE$.Integer2int(value2());
     */
    a = ObjectIntTest.value()
    a = ObjectIntTest.value2()
    a = b
    a = value2()
    println(a)
  }

}

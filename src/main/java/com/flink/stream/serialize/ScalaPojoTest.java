package com.flink.stream.serialize;


public class ScalaPojoTest {
    public static void main(String[] args) {
        PojoClassSerializeSuite.ScalaPojo1 data = new PojoClassSerializeSuite.ScalaPojo1();
        data.age_$eq(1);
    }
}

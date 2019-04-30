package com.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment

object BatchDemoCrossScala {
    def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data1 = List("tong" ,"liu")

    val data2 = List("12","13")
    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)
//    text1.cross(text2).print()
        text1.union(text2)
}
}

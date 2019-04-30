package com.flink.streaming.streamAPI

import com.flink.streaming.custormSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.parallel.immutable
object StreamingDemoConnectScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    val text1 = env.addSource(new MyNoParallelSourceScala)
    val text2 = env.addSource(new MyNoParallelSourceScala)


    val text1_str1 = text1.map(line =>(Tuple1(line+"")))
    val text2_str = text2.map(line =>("str" , "" + line))

    val connectedStreams = text1.connect(text2_str)

    val result = connectedStreams.map(line1=>{line1},line2=>{line2})

    result.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")



  }

}

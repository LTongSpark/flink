package com.flink.batch

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.mutable.ListBuffer
object BatchBroadcastScala {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        //准备需要广播的数据
        val broadData = ListBuffer[Tuple2[String,Int]]()
        broadData.append(("tong" ,13))
        broadData.append(("tog" ,14))
        broadData.append(("ling" ,15))
        broadData.append(("to" ,16))


        //处理需要广播的数据
        val tupleData = env.fromCollection(broadData)
        val toBroadcastData = tupleData.map(tup =>{
            Map(tup._1 ->tup._2)
        })
     val text = env.fromElements("tong" ,"tog")

//        val result = text.map(new RichMapFunction[String,String] {
//            var allMap = Map[String ,Int]
//            override def map(value: String) = ???
//
//            override def open(parameters: Configuration): Unit =
//                super.open(parameters)
//        })
//
//    }
    }
}

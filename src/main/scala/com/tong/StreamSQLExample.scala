package com.tong

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object StreamSQLExample {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val tableEnv = TableEnvironment.getTableEnvironment(env)
        val orderA:DataStream[wc] = env.fromCollection(Seq(
            wc("tong" ,1),
            wc("ting" ,1),
            wc("ting", 1)
        ))
        val orderB:DataStream[wc] = env.fromCollection(Seq(
            wc("liu" ,1),
            wc("tong" ,1)
        ))
        val tableA = tableEnv.fromDataStream(orderA ,'word ,'age)
        val table = tableEnv.sqlQuery(s"select * from $tableA")
        table.toAppendStream[wc].keyBy(0).sum(1).print()
        env.execute()
    }
    case class wc(word:String ,age:Int)
}

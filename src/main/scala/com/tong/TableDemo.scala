package com.tong

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment

object TableDemo {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val tEnv = TableEnvironment.getTableEnvironment(env)
        val input = env.fromElements(WC("hello" ,1),WC("world" ,2) ,WC("world" ,3))
        import org.apache.flink.table.api.scala._
        val table  = input.toTable(tEnv)
        val result = table.groupBy('word)
            .select('word ,'frequency.sum as 'frequency)
            .filter('frequency === 5)
            .toDataSet[WC]
            .print()
    }

    case class WC(word: String, frequency: Long)
}

package com.tong

import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.api.scala._


object flinkDemo {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val json = env.readTextFile("D:\\mr\\tong.txt")
        val wer = json.flatMap(_.toLowerCase.split("\\W+"))
            .filter(_.nonEmpty)
            .map((_, 1))
            .groupBy(0)
            //第一种直接用sum
//            .sum(1)
            //第二种直接用reduce
            .reduce((a,b) =>(a._1,a._2 + b._2))
                .print()

    }
}

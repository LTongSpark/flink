package com.flink.batch

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
object BatchWordCountScala {
    def main(args: Array[String]): Unit = {
        val inputPath = ""
        val outPath = ""

        val env = ExecutionEnvironment.getExecutionEnvironment
        val text = env.readTextFile(inputPath)


        text.flatMap(_.split("\t")).map((_,1)).groupBy(0)
            .reduce((a,b) =>(a._1 ,a._2 + b._2))
            .setParallelism(2)
            .print()
        env.execute("batch word count")
    }

}

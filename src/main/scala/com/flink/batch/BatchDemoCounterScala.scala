package com.flink.batch

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BatchDemoCounterScala {
    def main(args: Array[String]): Unit = {
        val env =   ExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._
        val data = env.fromElements("tong" ,"liu")

        val res = data.map(new RichMapFunction[String ,String] {
            //定义累加器
            val num = new IntCounter

            override def open(parameters: Configuration): Unit ={
                super.open(parameters)

            //注册累加器
            getRuntimeContext.addAccumulator("num_lines" ,this.num)
            }
            override def map(value: String) = {
                this.num.add(1)
                value
            }
        }).setParallelism(1)

        res.flatMap(_.split("\t")).map((_,1)).groupBy(0).sum(1).writeAsCsv("D:\\mr\\count.txt")
            .setParallelism(1)
        val jobResult = env.execute("tong")
        //获得累加器

        val numAcc = jobResult.getAccumulatorResult[Int]("num_lines")
        print("num" + numAcc)
    }

}

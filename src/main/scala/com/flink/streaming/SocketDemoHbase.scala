package com.flink.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, client}
//必须添加这一行，否则会报错
import org.apache.flink.streaming.api.scala._

object SocketDemoHbase {
    def main(args: Array[String]): Unit = {
        val port: Int = try {
            ParameterTool.fromArgs(args).getInt("port")
        } catch {
            case e: Exception => {
                //print("thiw is not port")
            }
                //默认值
                8888
        }
        //加载运行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //执行的顺序
        val socket: DataStream[String] = env.socketTextStream("zytc221", port)
        socket.flatMap(lines => lines.split(" "))
            .map((_, 1))
            .keyBy(0)
            .timeWindow(Time.seconds(10))
            .sum(1)
            .addSink(new myMysql)
            //.print().setParallelism(1)
        //执行的任务
        env.execute("socketDemo")

    }
    class myMysql extends RichSinkFunction[Tuple2[String, Int]] {
        private var conf: org.apache.hadoop.conf.Configuration = null
        private var conn : client.Connection = null
        override def open(parameters: Configuration): Unit = {
            conf = HBaseConfiguration.create()
            conf.set("hbase.rootdir", "hdfs://mycluster/hbase")
            conf.set("hbase.zookeeper.quorum", "zytc222:2181,zytc223:2181,zytc224:2181")
            conn = ConnectionFactory.createConnection(conf)
            super.open(parameters)
        }

        override def close(): Unit = {
            if(conn != null){
                try {
                    conn.close()
                }catch {
                    case e : Exception =>e.fillInStackTrace()
                }
            }
            super.close()
        }
        override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
            try {
                val word = value._1
                val count = value._2
                val table = conn.getTable(TableName.valueOf("wordcount"))
                val put = new Put(Bytes.toBytes(word))
                put.addColumn(Bytes.toBytes("qwe") ,Bytes.toBytes("word") ,Bytes.toBytes(word))
                put.addColumn(Bytes.toBytes("qwe") ,Bytes.toBytes("count") ,Bytes.toBytes(count))
                table.put(put)

            } catch {
                case e: Exception => e.printStackTrace()
            }
        }
    }
}

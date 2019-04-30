package com.flink.streaming

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
//必须添加这一行，否则会报错
import org.apache.flink.streaming.api.scala._

object SocketDemoEs {
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
        val socket: DataStream[String] = env.socketTextStream("192.168.1.221", port)
        val rdd:DataStream[(String, Int)] = socket.flatMap(lines => lines.split(" "))
            .map((_, 1)).keyBy(0).sum(1)
        val httpHosts: util.ArrayList[HttpHost] = new java.util.ArrayList[HttpHost]
         httpHosts.add(new HttpHost("zytc221", 9200, "http"))
        val esSinkBuilder = new ElasticsearchSink.Builder[Tuple2[String,Int]](
            httpHosts,
            new ElasticsearchSinkFunction[Tuple2[String,Int]]{
                def createIndexRequest(element:Tuple2[String,Int]): IndexRequest ={
                    val json = new java.util.HashMap[String, Any]
                    json.put("word" ,element._1)
                    json.put("count",element._2)
                    val id = element._1 + "_" + element._2
                    return Requests.indexRequest()
                        .index("flink_home")
                        .`type`("wordcount")
                        .id(id)
                        .source(json)
                }
                override def process(t: (String, Int), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
                    requestIndexer.add(createIndexRequest(t))
                }
            }
        )
        esSinkBuilder.setBulkFlushMaxActions(1)
        rdd.addSink(esSinkBuilder.build())
        env.execute("SocketDemoEs")

    }

}

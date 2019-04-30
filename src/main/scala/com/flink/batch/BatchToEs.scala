package com.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import scala.collection.mutable.ListBuffer

object BatchToEs {
    def main(args: Array[String]): Unit = {
       val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._

        val data = ListBuffer[Tuple2[Int, String]]()
        data.append((2, "zs"))
        data.append((4, "ls"))
        data.append((3, "ww"))
        data.append((1, "xw"))
        data.append((1, "aw"))
        data.append((1, "mw"))

        val text = env.fromCollection(data)
        val rdd: DataSet[(Int, String)] = text.first(3)

//        //把计算的结果存储到es中
//        val httpHosts = new java.util.ArrayList[HttpHost]
//        httpHosts.add(new HttpHost("hadoop100", 9200, "http"))


//        val esSinkBuilder = new org.apache.flink.streaming.connectors.elasticsearch2.
//        ElasticsearchSink.Builder[Tuple4[String, String, String, Long]](
//            httpHosts,
//            new ElasticsearchSinkFunction[Tuple4[String, String, String, Long]] {
//                def createIndexRequest(element: Tuple4[String, String, String, Long]): IndexRequest = {
//                    val json = new java.util.HashMap[String, Any]
//                    json.put("time", element.f0)
//                    json.put("type", element.f1)
//                    json.put("area", element.f2)
//                    json.put("count", element.f3)
//
//                    val id = element.f0.replace(" ", "_") + "-" + element.f1 + "-" + element.f2
//
//                    return Requests.indexRequest()
//                        .index("auditindex")
//                        .`type`("audittype")
//                        .id(id)
//                        .source(json)
//                }
//
//                override def process(element: Tuple4[String, String, String, Long], runtimeContext:
//                RuntimeContext, requestIndexer: RequestIndexer) = {
//                    requestIndexer.add(createIndexRequest(element))
//                }
//            }
//        )

    }

}

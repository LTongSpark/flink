package com.flink.spark_es

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming.EsSparkStreaming

/**
  * 根据es官网的描述，集成需要设置：
  * es.index.auto.create--->true
  * 我们要去连接es集群，要设置es集群的位置(host, port)
  */
object socketToEsStreaming {
    case class wordcount(word:String,count:Int)
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("Wordcount")
            .setMaster("local[4]")
            .set("es.index.auto.create", "true")
            .set("es.nodes", "s112")
        //---->如果是连接的远程es节点，该项必须要设置
            .set("es.port", "9200")
        val ssc = new StreamingContext(conf, Seconds(5))
        val streamData: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.126.112", 8888)
       //val value: DStream[String] = streamData.map(rdd => (rdd))
        val es:DStream[(String, Int)] = streamData.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
        val word  = es.map(rdd =>wordcount(rdd._1,rdd._2))
        EsSparkStreaming.saveToEs(word,"saprk/logs")
        ssc.start()
        ssc.awaitTermination()
    }
}


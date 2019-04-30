package com.tong
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

//import org.elasticsearch.spark.streaming.EsSparkStreaming

/**
  * 根据es官网的描述，集成需要设置：
  * es.index.auto.create--->true
  * 我们要去连接es集群，要设置es集群的位置(host, port)
  */
object App {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("Wordcount")
            .setMaster("local[4]")
            .set("es.index.auto.create", "true")
            .set("es.nodes", "zytc221")
        //---->如果是连接的远程es节点，该项必须要设置
            .set("es.port", "9200")

        val ssc = new StreamingContext(conf, Seconds(5))
        val streamData = ssc.socketTextStream("192.168.1.221", 8888)
        //val es:DStream[(String, Int)] = streamData.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
//        EsSparkStreaming.saveToEs(streamData,"spark_home/wordcount")
        ssc.start()
        ssc.awaitTermination()

    }
}


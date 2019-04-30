package com.flink.spark_es

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

object WordCountToEsStatic {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("wcScala")
        conf.setMaster("local[4]")
            .set("es.index.auto.create", "true")
            .set("es.nodes", "s112")
            //---->如果是连接的远程es节点，该项必须要设置
            .set("es.port", "9200")
        //创建spark上下文
        val sc: SparkContext = new SparkContext(conf)
        val text = sc.parallelize(80 to 90)
        val map: RDD[Int] = text.filter(_ % 2 == 0)
        val rdd1 = map.map(wordcount(_, "even"))
        val rdd2 = rdd1.map(x => word(wordcount(x.num, x.word), x.num))
        val rdd3 = rdd2.map(x => word1(word(wordcount(x.wordcount.num, x.wordcount.word), x.qwe), x.wordcount.num))
        EsSpark.saveToEs(rdd3, "spark_home/tong", Map("es.mapping.id" -> "word.wordcount.num"))
    }

    case class wordcount(num: Int, word: String)

    case class word(wordcount: wordcount, qwe: Int)

    case class word1(word: word, asd: Int)

}


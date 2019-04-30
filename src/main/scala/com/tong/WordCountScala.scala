package com.tong

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark
object WordCountScala {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("wcScala")
            .setMaster("local[4]")
            .set(ConfigurationOptions.ES_PORT,ConfigurationOptions.ES_PORT_DEFAULT)
            .set(ConfigurationOptions.ES_INDEX_AUTO_CREATE ,ConfigurationOptions.ES_INDEX_AUTO_CREATE_DEFAULT)
            .set(ConfigurationOptions.ES_NODES, "s112")
            .set(ConfigurationOptions.ES_NODES_WAN_ONLY,ConfigurationOptions.ES_NODES_WAN_ONLY_DEFAULT)

        //创建spark上下文
        val sc: SparkContext = new SparkContext(conf)
//        val text = sc.parallelize(20 to 30)
//        val map:RDD[Int] = text.filter(_%2 == 0)
//        val rdd1 = map.map(wordcount(_,"even"))
//        val rdd2 = rdd1.map(x =>word(wordcount(x.num,x.word),x.num))
//        val rdd3 = rdd2.map(x => word1(word(wordcount(x.wordcount.num,x.wordcount.word),x.qwe),x.wordcount.num))
//        EsSpark.saveToEs(rdd3,"hadoop/spark_qwe", Map("es.mapping.id" ->"word.wordcount.num"))

        val value: RDD[(String, collection.Map[String, AnyRef])] = EsSpark.esRDD(sc, "spark_home/tong")
        println(value.collect().toBuffer)

    }
    case class wordcount(num:Int,word:String)
    case class word(wordcount: wordcount,qwe:Int)
    case class word1(word: word,asd:Int)
}

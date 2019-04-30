package com.tong

import java.sql.DriverManager

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object mainApp {
    def createConn() = {
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection("jdbc:mysql://192.168.231.1:3306/big9", "root", "root")
    }
    def main(args: Array[String]): Unit = {
        val schema = StructType(
            List(
                StructField("id", DataTypes.IntegerType, false),
                StructField("name", DataTypes.StringType, true)
            )
        )
        val path = Utility.parseConfFile(args(0))
        val conf = new SparkConf()
            .setAppName("Wordcount")
        val ssc = new StreamingContext(conf ,Seconds(path.getInt("batch_period")))
        //kafka参数
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "zytc222:9092,zytc223:9092,zytc224:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "g1",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val topics = Array("topic11")
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        val ds2 = stream.map(record => (record.key, record.value))
        ds2.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
            .foreachRDD(rdd =>{
                rdd.foreachPartition(it =>{
                    val conn = createConn()
                    val ppst = conn.prepareStatement("insert into wc(word,count) values (?,?)")
                    for(e <- it){
                        ppst.setString(1,e._1)
                        ppst.setInt(1,e._2)
                        ppst.executeUpdate()
                    }
                    conn.close()
                    ppst.close()
                })
            })
        ssc.start()
        ssc.awaitTermination()
    }

}

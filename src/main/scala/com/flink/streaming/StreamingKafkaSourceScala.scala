package com.flink.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object StreamingKafkaSourceScala {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //隐式转换

        import org.apache.flink.api.scala._

        //checkpOint
        //必须要设置的一个参数  检查点
        env.enableCheckpointing(50000)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
        env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

        //设置statebackend

        //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true));
        val topic1: List[String] = List("topic1", "topic2")
        val topic: String = "top_1"
        val prop: Properties = new Properties()
        prop.setProperty("bootstrap.servers", "hadoop110:9092")
        prop.setProperty("group.id", "co1")
        //单个topic
        val myConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), prop)
        //多个topic 必须是Java的list  必须加上这个隐式转换  不加的话  就没有这个asJava这个方法
        import scala.collection.JavaConverters._
        val myConsumer1: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](topic1.asJava, new SimpleStringSchema(), prop)
        myConsumer1.setStartFromGroupOffsets()
        val text = env.addSource(myConsumer)

        text.flatMap(_.split(" "))
            .map((_, 1))
            .keyBy(0)
            .timeWindow(Time.seconds(10))
            .sum(1)
            .addSink(new MyMysql)
        //            .print().setParallelism(1)
    }

    class MyMysql extends RichSinkFunction[Tuple2[String, Int]] {
        private var conn: Connection = null
        private var ppst: PreparedStatement = null
        private val driver: String = "com.mysql.jdbc.Driver"
        private val url: String = "jdbc:mysql://localhost:3306/spark_home"
        private val user: String = "root"
        private val pswd: String = "root"

        override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            Class.forName(driver)
            conn = DriverManager.getConnection(url, user, pswd)
            val sql: String = "insert into wordcount(word,count) values(?,?)"
            ppst = conn.prepareStatement(sql)
        }

        override def close(): Unit = {
            super.close()
            if (ppst != null) {
                ppst.close()
            }
            if (conn != null) {
                conn.close()
            }
        }

        override def invoke(value: (String, Int)): Unit = {
            try {
                val word: String = value._1
                val count: Int = value._2
                ppst.setString(1, word)
                ppst.setInt(2, count)
                ppst.executeUpdate()
            } catch {
                case e: Exception => e.printStackTrace()
            }
        }
    }

}
package com.flink.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
//必须添加这一行，否则会报错
import org.apache.flink.streaming.api.scala._

object SocketDemoMysql {
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
        //执行的任务
        env.execute("SocketDemoMysql")

    }

    class myMysql extends RichSinkFunction[Tuple2[String, Int]] {
        private var conn: Connection = null
        private var ppst: PreparedStatement = null
        private val driver: String = "com.mysql.jdbc.Driver"
        private val url: String = "jdbc:mysql://localhost:3306/spark_home"
        private val user: String = "root"
        private val pswd: String = "root"

        override def open(parameters: Configuration): Unit = {
            Class.forName(driver)
            conn = DriverManager.getConnection(url, user, pswd)
            val sql = "insert into wordcount(word,count) values(?,?)"
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
                val word = value._1
                val count = value._2
                ppst.setString(1, word)
                ppst.setInt(2, count)
                ppst.executeUpdate()
            } catch {
                case e: Exception => e.printStackTrace()
            }
        }
    }

}

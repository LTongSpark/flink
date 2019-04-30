package com.flink.batch

import java.sql.DriverManager

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.mutable.ListBuffer
object BatchDemoMapPartitionScala {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val data = ListBuffer[String]()

        data.append("hello hello word")
        data.append("hello word")

        val text = env.fromCollection(data)
        text.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
        .mapPartition(it =>{
            Class.forName("com.mysql.jdbc.Driver")
            val url = "jdbc:mysql://localhost:3306/spark_home"
            val user = "root"
            val pass = "root"
            val conn = DriverManager.getConnection(url, user, pass)
            val sql = "insert into wordcount(word,count) values(?,?)"
            val ppst = conn.prepareStatement(sql)
            for(e <- it){
                ppst.setString(1,e._1)
                ppst.setInt(2,e._2)
                ppst.executeUpdate()
            }
            it
        }).print()

    }

}

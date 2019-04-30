package com.tong

import java.sql.DriverManager

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object jsonText {

    def createNewConnection() = {
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://192.168.231.1:3306/big9", "root", "root")
        conn
    }

    def main(args: Array[String]): Unit = {
        val schema = StructType(List(
            StructField("id", DataTypes.StringType),
            StructField("license_plate_number", DataTypes.StringType),
            StructField("driver", DataTypes.StringType),
            StructField("alarm_time", DataTypes.StringType),
            StructField("alarm_continued_time", DataTypes.StringType),
            StructField("alarm_position", DataTypes.StringType)
        ))
        val sc = new SparkConf()
            .setMaster("local[*]")
            .setAppName("app")
        Logger.getLogger("org").setLevel(Level.ERROR)


        val path = "D:\\mobsf\\sparktrain\\src\\main\\resources\\test.json"
        val spark = SparkSession.builder().master("local[5]").appName("WordCount")
            .getOrCreate().sparkContext.textFile(path)

        spark.map(rdd => {
            val records = JSON.parseObject(rdd)
            //    val records = jsonFile.getJSONArray("RECORDS").getJSONObject(0)
            var id = records.getString("id")
            var license_plate_number = records.getString("license_plate_number")
            var driver = records.getString("driver")
            var alarm_continued_time = records.getString("alarm_continued_time")
            var alarm_position = records.getString("alarm_position")
            (id, license_plate_number, driver, alarm_continued_time, alarm_position)
        }).foreachPartition(rdd => {
            val conn = createNewConnection()
            // executed at the driver
            val ppst = conn.prepareStatement("insert into wc(id,license_plate_number,driver,alarm_continued_time,alarm_position" +
                ") values(?,?,?,?,?,?)")
            conn.setAutoCommit(false)
            for (e <- rdd) {
                ppst.setString(1, e._1)
                ppst.setString(2, e._2)
                ppst.setString(3, e._3)
                ppst.setString(4, e._4)
                ppst.setString(5, e._5)
                ppst.executeUpdate()
            }
            conn.commit()
            conn.close()
            ppst.close()
        })
    }
}

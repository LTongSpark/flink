package com.tong

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object mainApp1 {
    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)
        val conf = new SparkConf()
            .setAppName("add")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(List("a", "b", "c"))
        val rdd2 = sc.parallelize(List("e", "d", "c"))
        val rdd3 = sc.parallelize(List("1","2","3"))

        val rdd = rdd1.union(rdd2).union(rdd3)
    }
}

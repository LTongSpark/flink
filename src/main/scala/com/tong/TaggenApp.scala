package com.tong

/**
  * 标签生成App
  */

import scala.io.Source

object TaggenApp {
    def main(args: Array[String]): Unit = {
       //加载数据
        val it =Source.fromFile("d:/mr/temptags.txt").getLines()

        //解析json数据，生成list[(busid ,tag)]的集合
        var list1 : List[(String, String)] = Nil
        for (line <- it){
            val arr = line.split("\t")
            //判断数组有效
            if (arr != null && arr.length > 1){
                val busid = arr(0)
                val json = arr(1)
                val tags:java.util.List[String]  = JSONUtil.parseTag(json)

                //使用Scala的自动转换。将Java集合转换成Scala的集合
                import scala.collection.JavaConversions._
                for (tag <- tags){
                    list1 = (busid,tag) :: list1
                }
            }
        }

        //统计每个商家对于每条评论的个数先分组在计算个数
        val list2 = list1.groupBy((t:Tuple2[String ,String])=> t)
        //list3 = ((busid,tag),count)
        val list3 = list2.map((e: Tuple2[Tuple2[String, String], List[Tuple2[String, String]]]) => (e._1, e._2.length)).toList

        //变换list3，形成(busid,(tag,count))
        val list4 = list3.map((e: Tuple2[Tuple2[String, String], Int]) => (e._1._1, (e._1._2, e._2)))

        //对busid进行分组，将同一商家的所有评论聚在一起
        val list5 = list4.groupBy((e: Tuple2[String, Tuple2[String, Int]]) => e._1)

        //变换集合,对value部分的(busid,(tag,count)) -> (tag,count)
        val list6 = list5.map((e: Tuple2[String, List[Tuple2[String, Tuple2[String, Int]]]]) => {
            val busid = e._1
            val list00 = e._2
            //去除了busid的(tag,count)列表。
            val list11 = list00.map((ee: Tuple2[String, Tuple2[String, Int]]) => ee._2)
            //按照评论数据降排序
            val list22 = list11.sortBy((e: Tuple2[String, Int]) => -e._2)
            (busid, list22)
        }).toList

        //对整个集合进行排序
        val list7 = list6.sortBy((e: Tuple2[String, List[Tuple2[String, Int]]]) => -e._2(0)._2)
        //输出结果
        for (t <- list5) {
            println(t)
        }

    }
}

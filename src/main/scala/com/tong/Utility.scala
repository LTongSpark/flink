package com.tong

import java.io.File

import com.alibaba.fastjson.JSONObject
import com.typesafe.config.{Config, ConfigFactory}

/**
  * 根据路径中的文件创建一个config对象
  */
object Utility {
    def parseConfFile(confFile: => String): Config = {
        val conf = ConfigFactory.parseFile(new File(confFile))
        conf
    }

    /**
      * Json字段解析
      *
      * @param jsonObj      json对象
      * @param fieldList    备选字段集合
      * @param defaultValue 默认值
      * @return
      */
    @annotation.tailrec
    def getJsonStr(jsonObj: JSONObject, fieldList: List[String], defaultValue: Object): Object = {
        var result = jsonObj.getOrDefault(fieldList.head, defaultValue);
        if (result != null && result != "") {
            result
        } else {
            if (fieldList.length < 2) defaultValue
            else getJsonStr(jsonObj, fieldList.tail, defaultValue)
        }
    }
}

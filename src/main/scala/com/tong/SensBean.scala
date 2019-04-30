package com.tong

import com.alibaba.fastjson.{JSON, JSONObject}

object SensBean {

    def operationBean(json: String) = {

        //1、原始json转化
        val json_text = JSON.parseObject(json)

        //2、定义并解析json日志字段
        //一级对象
        var Infotype = "" //日志类型(loadlog|authlog|sensitivelog)
        var Interface: Object = null //接口编号，用于区分日志来源
        var base: Object = null //基础信息
        var operation: Object = null //操作信息
        var optional: Object = null //选填字段
        //二级对象-base
        var ba_userId = "" //系统操作账号ID
        var ba_userType = "" //操作类型(manual系统人工|machine系统接口机器)
        var ba_hostname = "" //操作设备主机名
        var ba_client = "" //操作设备客户端
        var ba_ip = "" //操作IP地址
        var ba_timestamp = "" //12位系统时间戳记录
        var ba_originalLogId = "" //原始日志ID
        var ba_originalCMD = "" //原始日志操作记录(SQL语句、存储过程、菜单、功能按钮、api接口url等)
        //二级对象-operation
        var ope_type = "" //操作类型(data数据操作)
        var ope_act: Object = null //行为
        var ope_optional:Object = null
        var ope_abnormal = "" //异常情况记录&备注说明，字符串
        //二级对象-optional
        var opt_class = "" //涉及数据级别，1~4分别对应商密|受限|对内公开|对外公开级别
        var opt_databasename = "" //涉及数据库名
        var opt_tablename = "" //设及数据表名
        var opt_fieldname = "" //涉及字段名，用“,”分割
        //三级对象-operation-act
        var op_act_do = "" //操作(insert|update|select|delete|other)
        var op_act_useTime = "" //执行时间(单位毫秒)
        var op_act_acceft = "" //影响行数
        //do类型
        var doTypeList = List("insert", "select", "update", "delect", "other")

        //3、判断日志类型是否达标
        Infotype = getJsonStr(json_text, FieldRepository.fieldMap.get("infoType").get, "").toString
        operation = getJsonStr(json_text, FieldRepository.fieldMap.get("operation").get, null)
        if (operation != null) {
            ope_act = getJsonStr(operation.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ope_act").get, null)
        }
        if (ope_act != null) {
            op_act_do = getJsonStr(ope_act.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("op_act_do").get, "").toString
        }
        if (Infotype.toLowerCase() == "sensitivelog" || doTypeList.contains(op_act_do)) {
            //4、Json尝试解析
            Interface = getJsonStr(json_text, FieldRepository.fieldMap.get("Interface").get, null)
            base = getJsonStr(json_text, FieldRepository.fieldMap.get("base").get, null)
            optional = getJsonStr(json_text, FieldRepository.fieldMap.get("optional").get, null)

            if (base != null) {
                ba_userId = getJsonStr(base.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ba_userId").get, "").toString
                ba_userType = getJsonStr(base.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ba_userType").get, "").toString
                ba_hostname = getJsonStr(base.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ba_hostname").get, "").toString
                ba_client = getJsonStr(base.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ba_client").get, "").toString
                ba_ip = getJsonStr(base.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ba_ip").get, "").toString
                ba_timestamp = getJsonStr(base.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ba_timestamp").get, "").toString
                ba_originalLogId = getJsonStr(base.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ba_originalLogId").get, "").toString
                ba_originalCMD = getJsonStr(base.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ba_originalCMD").get, "").toString
            }

            if (operation != null) {
                ope_type = getJsonStr(operation.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ope_type").get, "").toString
                ope_abnormal = getJsonStr(operation.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("ope_abnormal").get, "").toString
            }

            if (optional != null) {
                opt_class = getJsonStr(optional.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("opt_class").get, "0").toString
                opt_databasename = getJsonStr(optional.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("opt_databasename").get, "").toString
                opt_tablename = getJsonStr(optional.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("opt_tablename").get, "").toString
                opt_fieldname = getJsonStr(optional.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("opt_fieldname").get, "").toString
            }

            if (ope_act != null) {
                op_act_useTime = getJsonStr(ope_act.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("op_act_useTime").get, "").toString
                op_act_acceft = getJsonStr(ope_act.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("op_act_acceft").get, "").toString
            }
            ope_optional = getJsonStr(operation.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("optional").get, null)
            if (ope_optional != null){
                opt_class = getJsonStr(ope_optional.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("opt_class").get, "0").toString
                opt_databasename = getJsonStr(ope_optional.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("opt_databasename").get, "").toString
                opt_tablename = getJsonStr(ope_optional.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("opt_tablename").get, "").toString
                opt_fieldname = getJsonStr(ope_optional.asInstanceOf[JSONObject], FieldRepository.fieldMap.get("opt_fieldname").get, "").toString
            }

        }

        //解析传递
        print(opt_class + opt_databasename + opt_tablename + opt_fieldname)

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

    def main(args: Array[String]): Unit = {
        var ss: String = "{\"Interface\":\"3000012005\",\"base\":{\"userId\":\"6223\",\"userType\":\"manual\",\"hostname\":\"\",\"client\":\"Chrome70.0\",\"ip\":\"106.38.97.146\",\"timestamp\":\"1543303603\",\"originalLogId\":\"0\",\"originalCMD\":\"DMS OP系统\"},\"Operation\":{\"act\":{\"dos\":\"xxxx\",\"useTime\":null,\"affect\":1},\"abnormal\":null},\"Optional\":{\"Class\":\"3\",\"Databasename\":\"EPBasicInfo\",\"Tablename\":\"V_DealerInfo\",\"Fieldname\":\"MemberLevelName,LocationName,LocationPath,ProviceName,CityName,LocationGroupName,BrandGroupName,BusinessModelName,DealerID,MemberLevelID,LocationID,DealerGroupID,BusinessModelID,BrandGroupID,DealerShortName,DealerFullName,DealerLoginName,DealerBusinessLicense,DealerRegisterAddress,DealerRegisterCapital,DealerContactAddress,DealerPostCode,DealerSalesPhone,DealerServicePhones,DealerComplainPhones,DealerRescuePhones,DealerFaxNumbers,DealerWebSiteUrl,DealerEmailAddress,DealerRemark,DealerStatus,DealerIsDelete,DealerCreateTime,DealerUpdateTime,DealerServiceManagerOPID,DealerServiceManagerOPName,MemberRecordIsTrail,DealerEnterpriseIntroduction,DealerAppType,MapProviderName,Longitude,Latitude,TrafficInfo,LocationGroupID,ProvinceID,CityID,IsFree,IsMember\"}}"
        val str = "{\"base\":{\"client\":\"python-requests/2.18.3\",\"hostName\":\"172.20.8.13\",\"ip\":\"172.20.8.1\",\"timestamp\":\"1543318862081\",\"userType\":\"manual\",\"originalLogId\":\"1234\",\"originalCMD\":\"wer\"},\"infoType\":\"sensitivelog\",\"interfaceNo\":\"3000060001\",\"operation\":{\"act\":{\"affect\":\"100000\",\"doType\":\"select\",\"useTime\":\"1\"},\"type\":\"系统操作\",\"abnormal\":\"567\"},\"optional\":{\"dataClass\":\"1\",\"tablename\":\"123\",\"databasename\":\"ert\",\"fieldname\":\"zxc\"}}"
        val st1 = "{\"base\":{\"client\":\"python-requests/2.18.3\",\"hostName\":\"172.20.8.13\",\"ip\":\"172.20.8.1\",\"timestamp\":\"1543318862081\",\"userType\":\"manual\",\"originalLogId\":\"1234\",\"originalCMD\":\"wer\"},\"infoType\":\"sensitivelog\",\"interfaceNo\":\"3000060001\",\"operation\":{\"act\":{\"affect\":\"100000\",\"doType\":\"select\",\"useTime\":\"1\"},\"optional\":{\"dataClass\":\"1\",\"tablename\":\"123\",\"databasename\":\"ert\",\"fieldname\":\"zxc\"},\"type\":\"系统操作\",\"abnormal\":\"567\"}}"
        println(SensBean.operationBean(st1))
        println(SensBean.operationBean(str))
    }
}

package com.tong

object FieldRepository {

  var fieldMap:Map[String,List[String]] = Map(
    "infoType" -> List("Infotype","infotype","infoType"),
    "Interface" -> List("Interface","interfaceNo","interface","interFace"),
    "base" -> List("base"),
    "operation" -> List("operation","Operation"),
    "optional" -> List("Optional","optional"),
    "ba_userId" -> List("userId","userid"),
    "ba_userType" -> List("userType"),
    "ba_hostname" -> List("hostname","hostName"),
    "ba_client" -> List("client"),
    "ba_ip" -> List("ip"),
    "ba_timestamp" -> List("timestamp"),
    "ba_originalLogId" -> List("originalLogId"),
    "ba_originalCMD" -> List("originalCMD"),
    "ope_type" -> List("type"),
    "ope_act" -> List("act"),
    "ope_abnormal" -> List("abnormal"),
    "opt_class" -> List("Class","dataClass"),
    "opt_databasename" -> List("databasename","Databasename","databaseName","dbName"),
    "opt_tablename" -> List("tablename","Tablename","tableName"),
    "opt_fieldname" -> List("fieldname","Fieldname"),
    "op_act_do" -> List("do","doType","action"),
    "op_act_useTime" -> List("useTime"),
    "op_act_acceft" -> List("affect")
  )

}

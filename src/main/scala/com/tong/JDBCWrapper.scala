package com.tong

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.ListBuffer

object JDBCWrapper {
    private var jdbcInstance: JDBCWrapper = _

    def getInstance(): JDBCWrapper = {
        synchronized {
            if (jdbcInstance == null) {
                jdbcInstance = new JDBCWrapper()

            }
        }
        jdbcInstance
    }

    class JDBCWrapper {
        val dbConnectionPool = new LinkedBlockingQueue[Connection] {
            try {
                Class.forName("com.mysql.jdbc.Driver")
            } catch {
                case e: ClassNotFoundException => e.printStackTrace()
            }
        }
        for (i <- 1 to 10) {
            try {
                val conn = DriverManager.getConnection("jdbc:mysql://192.168.231.1:3306/big9", "root", "root")
                dbConnectionPool.put(conn)
            } catch {
                case e: Exception => e.printStackTrace()
            }

        }

        def getConnection(): Connection = synchronized {
            while (0 == dbConnectionPool.size()) {
                try {
                    Thread.sleep(1)
                } catch {
                    case e: InterruptedException => e.printStackTrace()
                }

            }
            dbConnectionPool.poll()
        }

        def doBatch(sqlText: String, paramsList: ListBuffer[paramsList]): Array[Int] = {
            val conn: Connection = getConnection()
            var ppst: PreparedStatement = null
            val result: Array[Int] = null
            try {
                conn.setAutoCommit(false)
                ppst = conn.prepareStatement(sqlText)
                for (parameters <- paramsList) {
                    parameters.params_Type match {
                        case "adclickedinser" => {
                            ppst.setObject(1, parameters.params1)
                        }

                    }
                    ppst.addBatch()
                }
                val result = ppst.executeBatch()
                conn.commit()
            }
            catch {
                case e: Exception => e.printStackTrace()
            } finally {
                if (ppst != null) {
                    try {
                        ppst.close()
                    } catch {
                        case e: SQLException => e.printStackTrace()
                    }
                }
                if (conn != null) {
                    try {
                        dbConnectionPool.put(conn)
                    } catch {
                        case e: InterruptedException => e.printStackTrace()
                    }
                }
            }
            result

        }

        def doQuery(SqlText: String, paramsList: Array[_], callBack: ResultSet => Unit): Unit = {
            val conn: Connection = getConnection()
            var ppst: PreparedStatement = null
            var result: ResultSet = null
            try {
                ppst = conn.prepareStatement(SqlText)
                if (ppst != null) {
                    for (i <- 1 to paramsList.length - 1) {
                        ppst.setObject(i + 1, paramsList(i))
                    }
                }
                result = ppst.executeQuery()
                callBack(result)
            }catch {
                case e :Exception =>e.printStackTrace()
            }finally {
                if(ppst != null){
                    try {
                        ppst.close()
                    }catch {
                        case e : SQLException =>e.printStackTrace()
                    }
                }
                if(conn != null){
                    try{
                        dbConnectionPool.put(conn)
                    }catch {
                        case e:InterruptedException =>e.printStackTrace()
                    }
                }
            }

        }

        def resultCallBack(result: ResultSet ,blackListNames:List[String]):Unit ={

        }
        class paramsList extends Serializable{
            var params1:String = _
            var params2:String = _
            var params3:String = _
            var params4:String = _
            var params5:String = _
            var params6:String = _
            var params7:String = _
            var params10_Long:Long = _
            var params_Type:String = _
            var length:Int = _
        }

        class UserAdClicked extends Serializable{
            var timestamp: String = _
            var ip: String = _
            var userID: String = _
            var adID: String = _
            var province: String = _
            var city: String = _
            var clickedCount: Long = _

            override def toString: String = "UserAdClicked[timestamp" + timestamp + ",ip=" + ip +
                ",userID=" + userID + ",adID=" + adID + ",province=" + province +
            ",city=" + city  + ",clickedCount=" + clickedCount + "]"
        }



    }

}

package com.tong

import scala.reflect.io.File

object PrintString {
    def printString(str : String*): List[String] ={
        File
        var list :List[(String)] = Nil
        for (i <- str){
            list = i::list
        }
        list
    }

    def printMatch(x:Int)=x match {
        case 1 =>print(1)

    }

    def main(args: Array[String]): Unit = {
        for(i <- printString("tong", "liu ")){
            print(i)
        }
        print(printString("tong" ,"liu"))

    }

}

package com.seven.spark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang.StringUtils

/**
  *
  * Created by IntelliJ IDEA.
  * author   seven  
  * email    straymax@163.com
  * date     2018/5/16 上午10:37     
  */
object Hello {
  /**
    *
    * @param args
    *
    */
  def main(args: Array[String]): Unit = {
    println(new Date())
    println(StringUtils.isNotEmpty(" "))
    println("2018年".replaceAll("[^0-9.]",""))
    println("aa".replaceAll("[20051月,aaa]",""))
    println("201".replaceAll("[20051月,aaa]",""))
    println("YST180111451808181021336209".substring(0,4))
    println("YST180111451808181021336209".substring(3,11))
    println("YST180111451808181021336209".substring(11,13))
    println("YST180111451808181021336209".substring(13,15))
    println("YST180111451808181021336209".substring(15,17))
    println("YST180111451808181021336209".substring(17,19))
    println("YST180111451808181021336209".substring(19,21))
    println("YST180111451808181021336209".substring(21,23))
    println("YST180111451808181021336209".length)
    println("YST180111451808181021336209".substring("YST180111451808181021336209".length-4,"YST180111451808181021336209".length))

    val orderId = "YST180111451808181021336209"
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(df.parse("2018-01-01 00:00:01.0").getTime)

    val num = orderId.length
    val orderTime = "20" + orderId.substring(num-16, num-14) + "-" + orderId.substring(num-14, num-12) + "-" + orderId.substring(num-12, num-10) +
      " " + orderId.substring(num-10, num-8) + ":" + orderId.substring(num-8, num-6) + ":" + orderId.substring(num-6, num-4) + ".0"

    println(orderTime)
    println(df.parse(orderTime).getTime)

    //println("".replace("/平米/月",""))

    //println(patternShow(""))



//    val time = "2018-06-09 00:00:11.1"
//
//    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val long = df.parse(time).getTime
//    val flashback = new StringBuilder(String.valueOf(long)).reverse.toString()
//
//    println(flashback)
//
//    println("hello")
//    val ss = "adfad()#qag#gsag(faf)"
//    println(ss.replaceAll("[()#]",""))
  }

  def patternShow(x : Any) = x match {
    case 5 => "五"
    case true => "真"
    case "test" => "字符串"
    case null => "null值"
    case Nil => "空列表"
    case _ => "其他常量"
  }
}

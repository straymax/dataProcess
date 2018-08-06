package com.seven.spark

import java.text.SimpleDateFormat

/**
  *
  * Created by IntelliJ IDEA.
  *         __   __
  *         \/---\/
  *          ). .(
  *         ( (") )
  *          )   (
  *         /     \
  *        (       )``
  *       ( \ /-\ / )
  *        w'W   W'w
  *
  * author   seven  
  * email    sevenstone@yeah.net
  * date     2018/5/16 上午10:37     
  */
object Hello {
  /**
    *
    * @param args
    *
    */
  def main(args: Array[String]): Unit = {

    val time = "2018-06-09 00:00:11.1"

    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val long = df.parse(time).getTime
    val flashback = new StringBuilder(String.valueOf(long)).reverse.toString()

    println(flashback)

    println("hello")
    val ss = "adfad()#qag#gsag(faf)"
    println(ss.replaceAll("[()#]",""))
  }
}

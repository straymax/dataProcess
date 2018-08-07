package com.seven.spark.util

import java.util
import java.util.{Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
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
  * email    straymax@163.com
  * date     2018/5/24 上午11:03     
  */
object SimulationKafkaSendOutData {
  def main(args: Array[String]): Unit = {
    val path = "/yst/seven/orderHistory/*"
    val netPath = "/yst/seven/vem_nettype/*"
    val map = getDataByOrder(netPath)
//    simulationSendOutData(map)
    sendNetType(map)
  }

  def getDataByOrder(path: String): util.HashMap[Int, String] = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName))
    val data = sc.textFile(path).collect()
    val map = new util.HashMap[Int, String]()
    var num = 1

    for (d <- data) {
      map.put(num, d.replaceAll("[()]", ""))
      num += 1
    }
    sc.stop()
    map
  }

  def simulationSendOutData(map: util.HashMap[Int, String]): Unit = {
    println(map.size())
    val props = new Properties() //hadoop03,hadoop04,hadoop02,hadoop01
    //    props.put("bootstrap.servers", "vm-xaj-bigdata-da-d01:9092,vm-xaj-bigdata-da-d02:9092,vm-xaj-bigdata-da-d03:9092,vm-xaj-bigdata-da-d04:9092,vm-xaj-bigdata-da-d05:9092,vm-xaj-bigdata-da-d06:9092,vm-xaj-bigdata-da-d07:9092")
    props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092,hadoop04:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    for (n <- 1 to 300) {
      val start = System.currentTimeMillis
      for (m <- 1 to map.size()) {
        val message = map.get(m)
        producer.send(new ProducerRecord[String, String]("seven", n.toString+"_"+m.toString, message))
//        println(n.toString+"_"+m.toString)
//        Thread.sleep(100)
      }
      println(n+"\t"+(System.currentTimeMillis - start)+"ms")
    }
    producer.close()
  }



  def sendNetType(map: util.HashMap[Int, String]): Unit = {
    println(map.size())
    val props = new Properties() //hadoop03,hadoop04,hadoop02,hadoop01
    //    props.put("bootstrap.servers", "vm-xaj-bigdata-da-d01:9092,vm-xaj-bigdata-da-d02:9092,vm-xaj-bigdata-da-d03:9092,vm-xaj-bigdata-da-d04:9092,vm-xaj-bigdata-da-d05:9092,vm-xaj-bigdata-da-d06:9092,vm-xaj-bigdata-da-d07:9092")
    props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092,hadoop04:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
//    for (n <- 1 to 300) {
//      val start = System.currentTimeMillis
      for (m <- 1 to map.size()) {
        val message = map.get(m)
        producer.send(new ProducerRecord[String, String]("nettype", m.toString, message))
        //        println(n.toString+"_"+m.toString)
                Thread.sleep(10)
        println(m)
//      }61570
//      println(n+"\t"+(System.currentTimeMillis - start)+"ms")
    }
    producer.close()
  }
}

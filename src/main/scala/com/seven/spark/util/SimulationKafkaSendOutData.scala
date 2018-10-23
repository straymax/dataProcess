package com.seven.spark.util

import java.util
import java.util.Properties
import java.util.regex.Pattern

import com.seven.spark.kafka.KafkaSink
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by IntelliJ IDEA.
  * author   seven
  * email    straymax@163.com
  * date     2018/5/24 上午11:03     
  */
object SimulationKafkaSendOutData {
  private val log = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)


    if (args.length == 0 || args == null) {
      sparkConf.setMaster("local[2]")
    }

    val sc = new SparkContext(sparkConf)

    // 广播KafkaSink
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val brokers = "hadoop01:9092,hadoop02:9092,hadoop03:9092,hadoop04:9092"
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", brokers)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      log.warn("kafka producer init done!")
      sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

//        val path = "/yst/sta_vem/vem_order/main/*"

    val path = "/yst/sta_vem/vem_order/history/*"
    //    val map = getDataByOrder(path, sc)
    //    simulationSendOutData(map)
    //
    //    val path1 = "/yst/sta_vem/vem_order/history/201807*"
    //    val map1 = getDataByOrder(path1, sc)
    //    simulationSendOutData(map1)
    //
    //    val path2 = "/yst/sta_vem/vem_order/history/201808*"
    //    val map2 = getDataByOrder(path2, sc)
    //    simulationSendOutData(map2)
    //
    //    val path3 = "/yst/sta_vem/vem_order/history/201809*"
    //    val map3 = getDataByOrder(path3, sc)
    //    simulationSendOutData(map3)

    //    sc.stop()
    putKafka(path, sc, kafkaProducer)
//    getAccount(path,sc)

    //        sendNetType(map)
  }

  def getDataByOrder(path: String, sc: SparkContext): util.HashMap[Int, String] = {
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

  def putKafka(path: String, sc: SparkContext, kafkaProducer: Broadcast[KafkaSink[String, String]]): Unit = {

    sc.textFile(path).filter(x => {
      val line = x.split(",")
      val money = line(7)
      val id = line(0)
      if (id.length != 27 || StringUtils.isBlank(money)) { //过滤金额为空的数据,过滤订单长度不足的数据
        println(id)
        false
      } else {
        val regex = ".*[a-zA-Z]+.*"
        val str = id.substring(11, 27)
        val matcher = Pattern.compile(regex).matcher(str)
        !matcher.matches() //过滤订单后几位包含字母的数据
      }
    }).foreachPartition(row => {
      row.foreach(line => {
        kafkaProducer.value.send("seven", line)
//                println(line)
        Thread.sleep(2)
      })
    })
    //    sc.stop()
  }

  def getAccount(path: String, sc: SparkContext): Unit = {

    sc.textFile(path).filter(x => {
      val line = x.split(",")
      val account = line(10)
      "ocpQoxItg6WPnabGXn8vMOQuA5Wo".equals(account)
    }).foreachPartition(row => {
      row.foreach(line => {
        println(line)
      })
    })
    //    sc.stop()
  }

  def simulationSendOutData(map: util.HashMap[Int, String]): Unit = {
    val size = map.size()
    println(size)
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
    val start = System.currentTimeMillis
    for (m <- 1 to size) {
      val message = map.get(m)
      producer.send(new ProducerRecord[String, String]("seven", m.toString, message))
      //        println(n.toString+"_"+m.toString)
      Thread.sleep(1)
    }
    println((System.currentTimeMillis - start) + "ms")
    //    }
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

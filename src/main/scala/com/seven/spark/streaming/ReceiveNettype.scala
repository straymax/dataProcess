package com.seven.spark.streaming

import com.seven.spark.hbase.HBaseOps
import com.seven.spark.hbase.rowkey.RowKeyGenerator
import com.seven.spark.hbase.rowkey.generator.HashRowKeyGenerator
import com.seven.spark.streaming.ReceiveKafkaData.log
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


import scala.collection.JavaConversions._
//隐式转换

/**
  * Created by IntelliJ IDEA.  
  * author   seven  
  * email    straymax@163.com
  * date     2018/8/1 上午9:51     
  */
object ReceiveNettype {

  final private val log = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)

    if (args.length == 0 || args == null) {
      conf.setMaster("local[2]")
    }

    //创建streaming对象，5秒计算一次
    val ssc = new StreamingContext(conf, Seconds(5))

    receiveNetType(ssc)
  }

  def receiveNetType(ssc: StreamingContext): Unit = {
    val topics = Array("nettype")

    val brokers = "hadoop01:9092,hadoop02:9092,hadoop03:9092,hadoop04:9092"

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //获取实时数据
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val rowKeyGen: RowKeyGenerator[String] = new HashRowKeyGenerator()

    val family = "INFO"
    kafkaStream.map(_.value()).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(x => {
          var puts = List[Put]()
          x.foreach(row => {
            val line = row.split(",")
            val id = line(0)
            val name = line(1)
            val parentId = line(2)
            val createPerson = line(3)
            val createTime = line(4)
            val updatePerson = line(5)
            val updateTime = line(6)
            val isDelete = line(7)
            val netType = line(8)
            val other = row
            val rowKey = rowKeyGen.generate("") //获取rowkey
            val put = new Put(rowKey)
            //列族,列簇,数值
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ID"), Bytes.toBytes(id)) //插入id
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("NAME"), Bytes.toBytes(name)) //插入名字
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("PARENTID"), Bytes.toBytes(parentId)) //插入父id
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("CREATEPERSON"), Bytes.toBytes(createPerson)) //插入创建人
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("CREATETIME"), Bytes.toBytes(createTime)) //插入创建时间
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("UPDATEPERSION"), Bytes.toBytes(updatePerson)) //更新人
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("UPDATETIME"), Bytes.toBytes(updateTime)) //插入更新时间
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ISDELETE"), Bytes.toBytes(isDelete)) //是否正常
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("NETTYPE"), Bytes.toBytes(netType)) //插入类型
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("OTHER"), Bytes.toBytes(other)) //插入其他
            puts.::=(put)
            //orders.::=(order)
          })
          HBaseOps.puts("NETTYPE", puts) //HBase工具类，批量插入数据
          log.info(s"Inserting ${puts.size} lines of data to HBase is success . . .")
          //ElasticOps.puts("seven", "order", orders) //ES工具类，批量插入数据
          //log.info(s"Inserting ${orders.size} lines of data to ElasticSearch is success . . .")
        })
      }
    })
    ssc.start() //启动计算
    ssc.awaitTermination()
  }

}

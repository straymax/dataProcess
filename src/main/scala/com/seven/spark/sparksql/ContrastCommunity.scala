package com.seven.spark.sparksql

import java.util

import org.apache.commons.lang.time.StopWatch
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import scala.collection.mutable
/**
  * Created by IntelliJ IDEA.
  * author   seven  
  * email    straymax@163.com
  * date     2018/7/6 上午9:14
  *
  * 对比楼盘数据
  */
object ContrastCommunity {

  Logger.getLogger("org").setLevel(Level.ERROR)

  private final val log = LoggerFactory.getLogger(NetType.getClass)
  val spark = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[2]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    log.info("start . . .")
    val stopWatch = new StopWatch()
    stopWatch.start()

    val operatePath = "/yst/vem/operate/N/main/*"
    val operateMap = getNetTimeByOperate(operatePath)
    //广播
    val operateBv = spark.sparkContext.broadcast(operateMap)

    countContrast(operateBv)
    stopWatch.stop()
    log.info("success . . .")
    log.info("spend time " + stopWatch.toString + "ms")
  }

  /**
    * 获取网点运营天数,超过15天的网点
    *
    * @param path
    * @return
    */
  def getNetTimeByOperate(path: String): util.HashMap[String, String] = {
    val operate = spark.sparkContext.textFile(path).mapPartitions(x => {
      val hashMap = new mutable.HashMap[String, String]()
      x.foreach(row => {
        val line = row.replace("(", "").split(",")
        val netId = line(0)
        //网点id
        val time = line(2) //运营时间
        hashMap.put(netId, time)
      })
      hashMap.iterator
    }).filter(x => x._2.toDouble > 10).collect()

    val operateMap = new util.HashMap[String, String]
    for (o <- operate) {
      operateMap.put(o._1, o._2)
    }
    operateMap
  }

  def countContrast(broadcast: Broadcast[java.util.HashMap[String,String]]): Unit = {

    //获取运营map
    val operateMap = broadcast.value

    val community = spark.read //获取楼盘信息
      .option("header", "true") //保留头文件
      .option("delimiter", ",") //分隔符
      .csv("/yst/seven/community/*") //地址
      //更换列名
      //网点id,7日销量,城市,楼盘名称,地址,单价,住户数,建筑年代,物业公司,物业类型,物业费,开发商,面积
      .withColumnRenamed("CITY","city")//城市
      .withColumnRenamed("NETNM", "netName") //楼盘名称
      .withColumnRenamed("ADDRESS", "address")//地址
      .withColumnRenamed("PRICE", "price")//单价
      .withColumnRenamed("ZHS","houseNumber")//住户数
      .withColumnRenamed("JZND","year")//建筑年代
      .withColumnRenamed("WYGS","propertyCompany")//物业公司
      .withColumnRenamed("WYLX","propertyType")//物业类型
      .withColumnRenamed("WYF","propertyMoney")//物业费
      .withColumnRenamed("KFS","developer")//开发商
      .withColumnRenamed("ZJZMJ","acreage")//面积
      .select("city", "netName", "address", "price", "houseNumber", "year", "propertyCompany", "propertyType", "propertyMoney", "developer", "acreage") //指定查询列
//      .dropDuplicates(Seq("netName"))//去重
      .cache() //持久化

    community.show()

    community.createOrReplaceTempView("community") //注册临时表

    val net = spark.read //获取id和名称的关系
      .option("header", "true")
      .option("delimiter", ",")
      .csv("/yst/seven/data/netData/*")
      .select("id", "netName")
      .cache()

    net.createOrReplaceTempView("net_community")

    val statTest = spark.read
      .option("delimiter", ",")
      .csv("/yst/vem/sales/stat/N/main/*")
      .cache()

    val fieldSchema = StructType(Array(
      StructField("a0", StringType, true),
      StructField("a1", StringType, true),
      StructField("a2", StringType, true),
      StructField("a3", StringType, true),
      StructField("a4", StringType, true),
      StructField("a5", StringType, true),
      StructField("a6", StringType, true),
      StructField("a7", StringType, true),
      StructField("a8", StringType, true)
    ))

    spark.createDataFrame(statTest.rdd, fieldSchema).createOrReplaceTempView("statTest")

    def removeSymbol(str: String): String = {
      str.replaceAll("[()]", "")
    }

    spark.udf.register("removeSymbol", removeSymbol _) //注册自定义函数

    val stat = spark.sql("select removeSymbol(a0) as id,a2 as money from statTest").cache()

    stat.createOrReplaceTempView("stat")

    val sql =
      """
        |select s.id as id,n.netName as name,s.money from
        |stat s left join net_community n
        |on s.id = n.id
      """.stripMargin

    val data = spark.sql(sql)

    data.show()

    data.createOrReplaceTempView("data")


    val s = spark.sql("select distinct(d.id) as netId,d.money,c.* from data d left join community c on c.netName = d.name").cache()

    s.show()

    def isFlagOperate(str:String): Int ={
      if(operateMap.containsKey(str)){
         1
      }else{
        0
      }
    }

    spark.udf.register("isFlagOperate",isFlagOperate _)

    s.createOrReplaceTempView("table")

    val sparkSql =
      """
        |select * from table where city <> "" and city is not null and 1 = isFlagOperate(netId)
      """.stripMargin
    val ss = spark.sql(sparkSql).cache()


    ss.repartition(1).write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", ",")
      .csv("/yst/seven/data/community/")
//
//
//
//
//    val ss = spark.sql("select distinct(d.id) as netId,d.money,c.*  from data d right join community c on c.netName = d.name").cache()
//
//    ss.show()
//
//
//    ss.repartition(1).write.mode(SaveMode.Overwrite)
//      .option("header", "true")
//      .option("delimiter", ",")
//      .csv("/yst/seven/right/")

  }




}

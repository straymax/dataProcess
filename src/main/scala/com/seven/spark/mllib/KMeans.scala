package com.seven.spark.mllib

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession

/**
  * Created by IntelliJ IDEA.  
  *
  * @author seven  
  * @email straymax@163.com
  * @date 2019/5/29 1:56 PM    
  * @version 1.0
  */
object KMeans {
  val spark: SparkSession = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[2]")
    .config("spark.shuffle.sort.bypassMergeThreshold", "310")
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.default.parallelism", "300")
    .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "1024")
    .config("spark.shuffle.consolidateFiles", "true")
    .config("spark.hadoop.parquet.metadata.read.parallelism", "20")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate() //创建


  def main(args: Array[String]): Unit = {
    val rawdata = spark.read.option("header","true").csv("/yst/basic_data/seven/kmeans/customers_sale.csv")

    rawdata.show(5)

    rawdata.printSchema()

    //将数据由字符转化为数值型，并缓存
    val data = rawdata.select(
      rawdata("Channel").cast("Double"),
      rawdata("Region").cast("Double"),
      rawdata("Fresh").cast("Double"),
      rawdata("Milk").cast("Double"),
      rawdata("Grocery").cast("Double"),
      rawdata("Frozen").cast("Double"),
      rawdata("Detergents_Paper").cast("Double"),
      rawdata("Delicassen").cast("Double")
    ).cache()

    data.describe().show()

    //Channel、Region为类别型，其余6个字段为连续型，
    //为此，在训练模型前，需要对类别特征先转换为二元向量，然后，对各特征进行规范化。最后得到一个新的特征向量。
    //对类别特征转换为二元编码：
    val channelHot = new OneHotEncoder()
      .setInputCol("Channel")
      .setOutputCol("Channelvector")
      .setDropLast(false)

    val regionHot = new OneHotEncoder()
      .setInputCol("Region")
      .setOutputCol("Regionvector")
      .setDropLast(false)

    //把新生成的特征以及原来的六个组成一个特征向量
    val featuresArray = Array("Channelvector","Regionvector","Fresh","Milk","Grocery","Frozen","Detergents_Paper","Delicassen")

    //把源数据组合成特征向量faatures
    val vectorDf = new VectorAssembler()
      .setInputCols(featuresArray)
      .setOutputCol("features")
    //对特征进行规范化
    val scaledDf = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    val kMeans = new KMeans()
      .setFeaturesCol("scaledFeatures")
      .setK(4)//聚类个数
      .setMaxIter(50)//迭代次数默认二十
      .setSeed(123)


    val pipeline = new Pipeline()
      .setStages(Array(channelHot,regionHot,vectorDf,scaledDf))

    //把转化的二元向量、特征化转换等组装到流水线上，因pipeline中无聚类的评估函数，故，这里流水线中不纳入kmeans。具体实现如下：
    val dt = pipeline.fit(data).transform(data)

    //训练模型
    val model = kMeans.fit(dt)
    val results = model.transform(dt)


    //评估模型
    val wssse = model.computeCost(dt)
    println(s"Within Set Sum of Squared Errors = $wssse")
    //显示聚类结果。
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    results.collect().foreach(row => {println( row(10) + " is predicted as cluster " + row(11))})


    results.select("scaledFeatures","prediction").groupBy("prediction").count.show()



    //评估模型
    val ksse = (2 to 20 by 1).toList.map(k => {
      val kmeans = new KMeans().setFeaturesCol("scaledFeatures").setK(k).setSeed(123)
      val model = kmeans.fit(dt)
      //优化模型
      val wssse = model.computeCost(dt)
      // K，实际迭代次数，SSE，聚类类别编号，每类的记录数，类中心点
      (k,model.getMaxIter,wssse,model.summary.clusterSizes,model.clusterCenters)
    })

    //显示k、WSSSE评估指标，并按指标排序
    ksse.map(x => (x._1,x._3)).sortBy(x => x._2).foreach(println)

    //根据数据绘制出曲线图，观察平缓度，寻找最恰当的聚类个数
  }
}

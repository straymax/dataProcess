package com.seven.spark.mllib

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  * Created by IntelliJ IDEA.  
  *
  * @author seven  
  * @email straymax@163.com
  * @date 2019/5/29 4:22 PM    
  * @version 1.0
  */
object MovieALS {

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

    //加载数据
    val raings = spark.read.option("header", "true").csv("/yst/basic_data/seven/kmeans/ratings.csv")

    raings.show()

    raings.printSchema()


    //修改字段类型
    val raingsDF = raings.select(
      raings("userId").cast("Int"), //用户Id
      raings("movieId").cast("Int"), //电影Id
      raings("rating").cast("Double"), //电影评分
      raings("timestamp").cast("Long") //时间戳
    )
    raingsDF.printSchema()

    //将数据集分为训练集和测试集
    val Array(training, test) = raingsDF.randomSplit(Array(0.8, 0.2))

    //使用ALS在训练集数据上构建推荐模型
    //numBlocks是为了并行化计算而将用户和项目分割成的块的数量（默认为10）。
    //rank是模型中潜在因子的数量（默认为10）。
    //maxIter是要运行的最大迭代次数（默认为10）。
    //regParam指定ALS中的正则化参数（默认为1.0）。
    //implicitPrefs 显示的反馈ALS(true，显示的表示偏好程度)或者隐式的反馈ALS(false隐式指定偏好)。默认是false，显示反馈ALS
    //alpha 偏好观察中置信度(可理解为一个系数)，用于隐式反馈ALS。默认值是1.
    //nonnegative指定是否对最小二乘使用非负约束（默认为false）
    val als = new ALS()
      .setMaxIter(5) //最大迭代次数，默认10
      .setRegParam(0.01) //指定ALS中的正则化参数（默认为1.0）
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setColdStartStrategy("drop") //为确保不获取到NaN评估参数，我们将冷启动策略设置为drop。

    //如果评分矩阵是来自于另一个信息来源（即从其他信号推断出来），您可以将 implicitPrefs 设置为 true 以获得更好的结果：

    val model = als.fit(training)

    //通过计算rmse（均方根误差）来评估模型

    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error ： ${rmse}")



    //为用户推荐的前十个电影
    val userRecs = model.recommendForAllUsers(10)

    //为电影推荐的十个用户
    val movieRecs = model.recommendForAllItems(10)

    userRecs.show()
    movieRecs.show()
  }
}

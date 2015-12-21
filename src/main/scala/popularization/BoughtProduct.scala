package popularization

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.paukov.combinatorics.Factory

import scala.collection.JavaConverters._

object BoughtProduct {

  def main(args: Array[String]) {
    //    if (args.length < 1) {
    //      System.err.println("Usage: <recommend nums> 推荐个数 eg: 20")
    //      System.exit(1)
    //    }
    val sparkConf = new SparkConf().setAppName("PopularizationRecom").set("spark.default.parallelism", "24")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val dbName = "recsysdmd"
    import hiveContext.sql
    val purchase_sql = "select orderid,productid from purchase left outer join goods on purchase.goodsid = goods.id"

    println("Result of " + purchase_sql + ":")
    sql("use " + dbName)
    val sql_result = sql(purchase_sql)

    //1.拿到对应的数据
    val original_data = sql_result.map(x => {
      val orderid = x.get(0).toString
      val goodsid = x.get(1).toString
      (orderid, goodsid)
    })
    //2.得到对应订单中的产品集合
    val productOnOrder = original_data.reduceByKey(_ + "," + _)

    val data_mid = productOnOrder.flatMap(x => {
      //得到每个订单中产品的集合
      val proudcts = x._2.split(",").distinct
      val initialVector = Factory.createVector(
        proudcts)
      var index = 2
      if (proudcts.length < 2) {
        index = proudcts.length
      }
      val result = Factory.createSimpleCombinationGenerator(initialVector, index)
      result.generateAllObjects().asScala.toList.map {
        item =>
          item.asScala.toList.sorted
      }
    })
    //1、订单总数
    val totalOrders = productOnOrder.count()

    //2、统计各类 A->B 出现的次数
    val count_double = data_mid.filter(_.length == 2).map(x => {
      (x.mkString("@"), 1)
    }).reduceByKey(_ + _).cache()


    //统计各种商品在订单中出现的次数
    val product2Num = productOnOrder.flatMap(x => {
      x._2.split(",")
    }).map(x => {
      (x, 1)
    }).reduceByKey(_ + _).collectAsMap()

    //用2的结果/订单总数的到对应的结果   支持度: P(A∪B)，即A和B这两个项集在事务集D中同时出现的概率
    val productResult = count_double.flatMap(x => {
      val key = x._1
      val support = x._2.toDouble / totalOrders //支持度
      val _1 = x._1.split("@")(0)
      val _2 = x._1.split("@")(1)
      //置信度1: P(B｜A)，即在出现项集A的事务集D中，项集B也同时出现的概率。
      val confidenceP_B_A = x._2.toDouble / product2Num.get(_1).get
      //置信度2: P(A｜B)，即在出现项集A的事务集D中，项集B也同时出现的概率。
      val confidenceP_A_B = x._2.toDouble / product2Num.get(_2).get
      //相关度 p(A|B)/N   / p(A)/N * p(B)/N
      val correlation = x._2 * totalOrders.toDouble / product2Num.get(_1).get / product2Num.get(_2).get
      List(_1 + "\t" + _2 + "\t" + support + "\t" + confidenceP_B_A + "\t" + correlation,
        _2 + "\t" + _1 + "\t" + support + "\t" + confidenceP_A_B + "\t" + correlation)
    })
    overwriteTextFile("popular/product", productResult)
  }

  def deletePath(sc: SparkContext, path: String): Unit = {
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    val hdfsPath = new org.apache.hadoop.fs.Path(path)
    if (hdfs.exists(hdfsPath))
      hdfs.delete(hdfsPath, true)
  }

  def overwriteTextFile[T](path: String, rdd: RDD[T]): Unit = {
    deletePath(rdd.context, path)
    rdd.saveAsTextFile(path)
  }

}

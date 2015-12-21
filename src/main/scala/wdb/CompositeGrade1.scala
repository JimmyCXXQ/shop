package wdb

import org.apache.spark.{SparkConf, SparkContext}
import untils.ConnectionFactory

/**
 * Created by wangdabin1216 on 15/11/23.
 */
object CompositeGrade1 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("CompositeGrade1")
    val sc = new SparkContext(sparkConf)
    val data1 = sc.parallelize(Array((("A","B"),"2"),(("A","B"),"5"),(("A","B"),"1")))
    data1.map(x =>{
      ConnectionFactory.connect()
      (1,2)
    }).collect()
    sc.stop()
  }
}

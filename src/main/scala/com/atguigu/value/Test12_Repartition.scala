package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test12_Repartition {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 3)
    intRDD.repartition(5).mapPartitionsWithIndex((num,list) => list.map(i => (num,i)))
    .collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}

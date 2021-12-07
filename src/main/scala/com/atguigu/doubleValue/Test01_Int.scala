package com.atguigu.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test01_Int {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val intRDD1: RDD[Int] = sc.makeRDD(List(5, 6, 7, 8, 9, 10), 3)

    // 求交集
    // 最终的分区个数使用的是较多的rdd的分区数
    intRDD.intersection(intRDD1).mapPartitionsWithIndex((num,list) => list.map(i => (num,i)))
    .collect().foreach(println)

    // 求并集
    // 数据不发生变化  也不去重  只是把多个分区的数据合在一起  结果的分区个数是rdd分区个数之和
    intRDD.union(intRDD1).mapPartitionsWithIndex((num,list) => list.map(i => (num,i)))
    .collect().foreach(println)

    // 求差集
    // 结果会打散重新分区  走shuffle
    intRDD.subtract(intRDD1).mapPartitionsWithIndex((num,list) => list.map(i => (num,i)))
    .collect().foreach(println)

    Thread.sleep(1000000)
    //4.关闭连接
    sc.stop()
  }
}

package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Day03_04 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    rdd.glom()
      .map(array => array.mkString("-"))
      .mapPartitionsWithIndex((num,list) => list.map(i => (num,i)))
      .collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}

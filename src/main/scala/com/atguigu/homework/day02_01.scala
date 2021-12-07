package com.atguigu.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object day02_01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/day02.txt")
    rdd.flatMap(_.split(" "))
      .map(s => (s.charAt(0),1))
      .reduceByKey(_ + _)
      .foreach(println)

    //4.关闭连接
    sc.stop()
  }
}

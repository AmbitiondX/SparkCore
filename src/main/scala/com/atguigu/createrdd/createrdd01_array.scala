package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object createrdd01_array {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile("input")

    rdd1.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)

    sc.stop()
  }
}

package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test13_SortBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(10, 22, 13, 4, 5, 60, 17), 2)
    intRDD.sortBy(i => i).collect().foreach(println)

    val tupleRDD: RDD[(String, Int)] = sc.makeRDD(List(("hello", 10), ("world", 22), ("scala", 13), ("spark", 1)), 2)
    tupleRDD.sortBy(tuple => tuple._1).collect().foreach(println)
    println("=====================")
    tupleRDD.sortBy(tuple => tuple._2).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}

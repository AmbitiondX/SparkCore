package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test04_FlatMap {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9)))
    listRDD.flatMap(list => list).collect().foreach(println)

    listRDD.flatMap(list => list.map(("我是",_))).collect().foreach(println)

    val strRDD: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))
    strRDD.flatMap(_.split(" ")).collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }
}

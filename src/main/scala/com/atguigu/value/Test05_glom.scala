package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test05_glom {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    // 3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)


    // 3.2 求出每个分区的最大值  0->1,2   1->3,4
    rdd.glom().map(_.toList).mapPartitionsWithIndex((num,list) => list.map(i => (num,i))).collect().foreach(println)

    sc.textFile("input/sql.txt",1)
      .glom()
      .map(array => array.mkString("\n"))
      .collect().foreach(println)



    //4.关闭连接
    sc.stop()
  }
}

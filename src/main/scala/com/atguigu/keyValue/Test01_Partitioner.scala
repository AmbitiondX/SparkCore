package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Test01_Partitioner {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    // 一般的rdd是不能够使用keyValue类型的算子的
    // 必须是二元组类型的rdd才能够调用keyValue类型的算子
    intRDD.map((_,1))
      .partitionBy(new HashPartitioner(2))
      .mapPartitionsWithIndex((num,list) => list.map(i => (num,i)))
      .collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}

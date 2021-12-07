package com.atguigu.keyValue

import org.apache.spark.{SparkConf, SparkContext}

object Test03_ReduceByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2),("a",2),("b",4),("a",3),("b",6)),2)
    rdd.reduceByKey(_ + _)
      .mapPartitionsWithIndex((num,list) => list.map(i => (num,i)))
      .collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}

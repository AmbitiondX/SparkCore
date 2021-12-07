package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test06_GroupBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)




    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),3)
    val rdd1: RDD[(Int, Iterable[Int])] = intRDD.groupBy(_ % 2)
    rdd1.mapPartitionsWithIndex((num,list) => list.map(i => (num,i)))
    .collect().foreach(println)



//    rdd1.saveAsTextFile("groupBy")


    // 3.3 创建一个RDD
    val rdd2: RDD[String] = sc.makeRDD(List("hello","hive","hadoop","spark","scala"))

    // 3.4 按照首字母第一个单词相同分组
    rdd2.groupBy(str=>str.substring(0,1)).collect().foreach(println)



//    Thread.sleep(1000000000)
    //4.关闭连接
    sc.stop()
  }
}

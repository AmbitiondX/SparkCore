package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Partition01_Array_default {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5),3)


    //3. 输出数据，产生了12个分区
    rdd.saveAsTextFile("output")
    //结论:从集合创建rdd,如果不手动写分区数量的情况下,默认分区数跟本地模式的cpu核数有关
    //local : 1个   local[*] : 笔记本所有核心数    local[K]:K个

    //4.关闭连接
    sc.stop()
  }
}

package com.atguigu.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Checkpoint02 {
  def main(args: Array[String]): Unit = {
    // 设置访问hdfs集群的用户名
    System.setProperty("HADOOP_USER_NAME","atguigu")

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)



    // 设置checkpoint在hdfs上的存储路径
    sc.setCheckpointDir("hdfs://hadoop102:8020/checkpoint")

    val rdd1: RDD[String] = sc.textFile("input/1.txt")
    val rdd2: RDD[(String, Int)] = rdd1
      .flatMap(_.split(" "))
      .map(word => {
        println("#################################")
        (word, 1)})
    val rdd3: RDD[(String, Int)] = rdd2
      .reduceByKey(_ + _)
    val rdd4: RDD[(String, Int)] = rdd3
      .sortByKey()


    // 在这只检查点之前添加缓存，避免重复执行
    rdd2.cache()
    rdd2.checkpoint()
    println(rdd2.partitioner)
    println(rdd3.partitioner)
    println(rdd4.partitioner)

    rdd2.collect().foreach(println)
    rdd2.collect().foreach(println)

//    Thread.sleep(1000000)
    //4.关闭连接
    sc.stop()
  }
}

package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Partition03_file_default {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
    val sc: SparkContext = new SparkContext(conf)

    //1）默认分区的数量：默认取值为当前核数和2的最小值,一般为2
    val rdd: RDD[String] = sc.textFile("input",5)
/*
    minPartitions = 3
    12    0 1 2 3
    345   4 5 6 7 8
    6     9 10 11
    7     12 13 14
    89    15 16

    0  [0,5]    12  345
    1  [5,10]   6
    2  [10,15]  7 89
    3  [15,20]

    minPartitions = 5
    1     0 1 2
    23    3 4 5 6
    4     7 8 9
    567   10 11 12 13 14
    8     15 16 17
    9     18

    0  [0,3]    1 23
    1  [3,6]
    2  [6,9]    4
    3  [9,12]   567
    4  [12,15]  8
    5  [15,18]  9
    6  [18,18]
*/
    rdd.saveAsTextFile("output")

    sc.stop()
  }
}

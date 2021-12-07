package project

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object Test05_Top10_Acc {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val actionRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    val userVisitActionRDD: RDD[UserVisitAction] = actionRDD.map(datas => {
      val data: Array[String] = datas.split("_")
      UserVisitAction(
        data(0),
        data(1),
        data(2),
        data(3),
        data(5),
        data(5),
        data(6),
        data(7),
        data(8),
        data(9),
        data(10),
        data(11),
        data(12)
      )
    })

    val acc: MyAcc = new MyAcc
    sc.register(acc)
    userVisitActionRDD.foreach(s => acc.add(s))

    val map: mutable.Map[(String, String), Long] = acc.value
//    println(map)
    val categoryCountInfoes: immutable.Iterable[CategoryCountInfo] = map.groupBy(_._1._1)
    .map({
      case (id, list) =>
        CategoryCountInfo(id, list.getOrElse((id, "click"), 0L), list.getOrElse((id, "order"), 0L), list.getOrElse((id, "pay"), 0L))
    })

    categoryCountInfoes
      .toList
      .sortBy(info => {
        (info.clickCount,info.orderCount,info.payCount)
      })(Ordering[(Long, Long, Long)].reverse)
      .take(10)
      .foreach(println)

    //4.关闭连接
    sc.stop()
  }
  class MyAcc extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{
    val map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new MyAcc

    override def reset(): Unit = map.clear()

    override def add(v: UserVisitAction): Unit = {
      if (v.click_category_id != "-1"){
        map((v.click_category_id,"click")) = map.getOrElse((v.click_category_id,"click"),0L) + 1L
      } else if (v.order_category_ids != "null"){
        val orderIds: Array[String] = v.order_category_ids.split(",")
        orderIds.foreach(s => {
          map((s,"order")) = map.getOrElse((s,"order"),0L) + 1L
        })
      }else if (v.pay_category_ids != "null") {
        val payIds: Array[String] = v.pay_category_ids.split(",")
        payIds.foreach(s => {
          map((s,"pay")) = map.getOrElse((s,"pay"),0L) + 1L
        })
      }
    }

    override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
      val map1: mutable.Map[(String, String), Long] = other.value
      for (elem <- map1) {
        map(elem._1) = map.getOrElse(elem._1,0L) + elem._2
      }
    }

    override def value: mutable.Map[(String, String), Long] = map
  }
}

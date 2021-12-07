package project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test04_Top10 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val actionRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val filterRDD: RDD[String] = actionRDD.filter(s => {
      val strings: Array[String] = s.split("_")
      strings(5) == "null"
    })

    val userVisitActionRDD: RDD[UserVisitAction] = filterRDD.map(datas => {
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

    val categoryCountInfoRDD: RDD[CategoryCountInfo] = userVisitActionRDD.flatMap(obj => {
      if (obj.click_category_id != "-1") {
        List(CategoryCountInfo(obj.click_category_id, 1, 0, 0))
      } else if (obj.order_category_ids != "null") {
        obj.order_category_ids.split(",").map(s => CategoryCountInfo(s, 0, 1, 0))
      } else if (obj.pay_category_ids != "null") {
        obj.pay_category_ids.split(",").map(s => CategoryCountInfo(s, 0, 0, 1))
      } else Nil
    })

    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = categoryCountInfoRDD.groupBy(_.categoryId)

//    groupRDD.take(1).foreach(println)

    val resRDD: RDD[CategoryCountInfo] = groupRDD.map({
      case (id, list) => list.reduce((res, elem) => {
        res.clickCount += elem.clickCount
        res.orderCount += elem.orderCount
        res.payCount += elem.payCount
        res
      })
    })

    resRDD.sortBy(info => (info.clickCount,info.orderCount,info.payCount),false)
      .take(10).foreach(println)

/*    val flatMapRDD: RDD[(String, (Int, Int, Int))] = userVisitActionRDD.flatMap(obj => {
      if (obj.click_category_id != "-1") {
        List((obj.click_category_id, (1, 0, 0)))
      } else if (obj.order_category_ids != "null") {
        obj.order_category_ids.split(",").map(s => (s, (0, 1, 0)))
      } else if (obj.pay_category_ids != "null") {
        obj.pay_category_ids.split(",").map(s => (s, (0, 0, 1)))
      } else Nil
    })*/

//    flatMapRDD.take(20).foreach(println)

/*
    flatMapRDD
      .reduceByKey((t1,t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))
      .sortBy(_._2,false)
      .take(10)
      .foreach(println)
*/

    //4.关闭连接
    sc.stop()
  }
}

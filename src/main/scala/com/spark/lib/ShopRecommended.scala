package com.spark.lib

import org.apache.spark.{SparkConf, SparkContext}

class ShopRecommended {
  def main(args: Array[String]): Unit = {

    val conf =new SparkConf().setMaster("local")
      .setAppName(getClass.getSimpleName)
    val sc=new SparkContext(conf)
    val users=sc.parallelize(Array("aa","bb","cc","dd","ee"))
    val films=sc.parallelize(Array("钢铁侠","复仇者联盟","那些年","四四","事实上"))
    def getcollaboratesource(use1:String,user2:String):Double={
     // val usefilmsourece1=users.get
      1.2


    }
  }

}

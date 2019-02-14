package com.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object GroupSort2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setAppName(getClass.getName)
      .setMaster("local[2]")

    val sc=SparkContext.getOrCreate(conf)

    val text=sc.textFile("data/groupsort.txt")
    val pair=text.map(line=>line.split(" "))
      .filter(l=>l.length>=2)
      .map(word=> {
        val name = word(0)
        val count =if(word.nonEmpty)word(1).toInt else 0
        (name, count)
      })

      val res1=pair.groupByKey()
      .map(c=>({
        val key=c._1
        val top2=c._2.toList.sorted.takeRight(2).reverse
        (key,top2)
      }))

   res1.collect().foreach(i=>println(i._1+"============="+i._2.mkString("[",",","]")))
//第二种算法
    val res2: RDD[(String, ArrayBuffer[Int])] =pair.aggregateByKey(new ArrayBuffer[Int]())(
      (u1,v1) =>{
        u1 += v1
        u1.sorted.takeRight(5)
      },
      (u1,u2)=>{
        u1 ++= u2
        u1.sorted.takeRight(5).reverse
      }
    )
 res2.collect.foreach(i=>println(i._1+" ---------"+i._2.mkString(",")))

}

}

package com.spark.kafka

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDidrectwc1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Steaming WordCount Demo")
      .setMaster("local[2]")

    conf.set("spark.streaming.backpressure.enabled","true")//开启背压功能
    conf.set("spark.streaming.kafka.maxRatePerPartition","100")

    val sc = SparkContext.getOrCreate(conf)

    val ssc = new StreamingContext(sc,Seconds(5))

  }
}

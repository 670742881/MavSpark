package com.spark.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object  KafkaReceiverStreamingWC1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Steaming WordCount Demo")
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)

    val ssc = new StreamingContext(sc,Seconds(5))

    val kafkaParam=Map( "zookeeper.connect" -> "bigdata.server1:2181",
      "group.id" -> "01",
      "zookeeper.connection.timeout.ms" -> "10000",
      "auto.offset.reset"->"smallest")
    val topic=Map("test"->3)

    val line=KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
      ssc,
      kafkaParam,
      topic,
      StorageLevel.MEMORY_AND_DISK_SER_2
    ).map(_._2)

    //调用Dtreaming转换操作实现需求
    val wordcount=line.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)
    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }


}

package com.spark.com.sparkStreaming

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object

KafkaReceiverWC1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("sparkStreaming 和 kafka 集成方式：基")
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)

    val ssc = new StreamingContext(sc,Seconds(5));

    //第1步，定义数据源创建InputStreaminng
    /**
      * sparksteaming支持的数据源有两个类别：
      *   --1.基础数据源 由streamingcontext提供的api  socketTextStream
      *   --2.高级数据源 由外部工具类提供 kafak
       */
      //基于接收器的方式使用的是kafak消费者的高级api（High Level）,消费者的offset由zookeeper保管，给定对应消费者的配置参数
   val kafkaParams = Map(
      "zookeeper.connect" -> "bigdata.server1:2181",   //连接zookeeper的地址，获取和提交offet
      "group.id" ->"KafkaReceive",             //消费组的名称
      "zookeeper.connection.timeout.ms" -> "10000",
      "auto.offset.reset"-> "smallest"    //当前sparksreaing对应的消费者组第一次消费的时候方式，当前是从头消费
    )

    //指定当前消费的kafaka的topic以及对应的分区数
    val topics = Map("test" -> 1)

    //K type of Kafka message key //消息/数据的key
    //V type of Kafka message value //消息/数据的value
    //U type of Kafka message key decoder  //消息/数据的key化的编码器
    //T type of Kafka message value decoder //消息/数据的value用于反序列化的编码器
    val lines: DStream[String] = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
        ssc,
        kafkaParams,
        topics,
        StorageLevel.MEMORY_AND_DISK_SER_2
    ).map(_._2)

    //第2步 调用DStream的转换操作实现计算需求
    val wordcount = lines
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)

    //第3步 调用DStrem的输出操作进行打印或者保存
    wordcount.print()

    //第4步 接收并处理数据
    ssc.start()

    //第5步 等待被终止
    ssc.awaitTermination()

  }
}

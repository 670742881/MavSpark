package com.spark.com.sparkStreaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDriectWC1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Steaming WordCount Demo")
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)

    val ssc = new StreamingContext(sc,Seconds(5))
    //第1步，定义数据源创建InputStreaminng

    //由于Direct方式的kafka和Spark Streaming的集成方式中采用的api是低级封装的api（low lever api），消费的offset信息不需要zookeeper保存，而是直接去找broker节点
    val kafkaParams = Map(
      "metadata.broker.list"->"bigdata.server1:9092,bigdata.server1:9093,bigdata.server1:9094,bigdata.server1:9095"
    )

    //由于Direct方式的kafka和Spark Streaming的集成方式中采用的api是低级封装的api（low lever api），此时消费者的offet，由自己保管，不再是zookeeper，同时还可以自己指定从哪个offet开始消费 ，指定消费的topic以及对应每个分区，开始消费的offset
    val fromOffsets:Map[TopicAndPartition, Long] = Map(
      TopicAndPartition("test1",0) -> 0,
      TopicAndPartition("test1",1) -> 100,
      TopicAndPartition("test1",2) -> 200,
      TopicAndPartition("test1",3) -> 300
    )

    //MessageAndMetadata可以同时获取message的所属的topic，partiron，offset等元数据，也可以获取key和value，这里仅需要value
    val messageHandler: MessageAndMetadata[String, String] => String = (mmd:MessageAndMetadata[String, String])=>{
      //Messaged的Metadata
     // mmd.topic
     // mmd.partition
     // mmd.offset
      //Messaged本身
      //mmd.key()
      mmd.message()
    }

    val lines: InputDStream[String] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,String](
      ssc,
      kafkaParams,
      fromOffsets,
      messageHandler
    )

    //第2步，调用DStream的transform操作，完成需求
    val wordcount = lines.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)



    //第3步，调用输出操作，对结果进行打印或者保存
    wordcount.print()

    //  wordcount.saveAsTextFiles("hdfs://bigdata.server1:8020/spark/streamOut-",suffix = s"${System.currentTimeMillis()}")

    //第4步，启动接收数
    ssc.start()


    //第5步，等待被终止
    ssc.awaitTermination()

  }
}

package com.spark.sparkStreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object UpdateStateStreamingWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Steaming WordCount Demo")
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)

    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("checkpoint")

    /**
      * updateStateBykey：有状态的转换操作，通常用于进行实时累计统计，比如实时统计累计的订单总数，成交额
      * 需求：累计统计单词的个数
      */

    //第1步，通过基础数据源读取数据形成inputDsteam
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata.server1",9999)

    //第2步，调用DStream的transform操作，完成需求
    val wordcount = lines.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)
      .updateStateByKey((seq:Seq[Int],state:Option[Long])=>{
        //1.当前批次中相同key的value形成的有序集合 Seq（1，1，1，1，1）
        val currentValue: Int = seq.sum
        //2.获取这个key在之前所有批次的累计结果
        val previousValue: Long = state.getOrElse(0)
        //3.更新操作
        Option.apply(currentValue+previousValue)
      })



    //第3步，调用输出操作，对结果进行打印或者保存
    wordcount.print()


    //第4步，启动接收数
    ssc.start()


    //第5步，等待被终止
    ssc.awaitTermination()


  }

}

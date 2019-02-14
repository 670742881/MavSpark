package com.spark.com.sparkStreaming

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Steaming WordCount Demo")
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)

    val ssc = new StreamingContext(sc,Seconds(5))

    //第1步，通过基础数据源读取数据形成inputDsteam
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata.server1",9999)

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

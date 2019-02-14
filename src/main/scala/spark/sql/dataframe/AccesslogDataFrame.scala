package spark.sql.dataframe

import com.spark.dataFrame.ApacheAccessLog
import org.apache.spark.sql.SparkSession


object AccesslogDataFrame {

  def main(args: Array[String]): Unit = {

     val spark=SparkSession
      .builder()
      .appName("AccesslogDataFrame")
      .master("local[2]")
      .getOrCreate()

    //创建sparkContext
    val sc=spark.sparkContext
    val logFile = "data/access_log"
    val accessLogsRdd = sc
      .textFile(logFile)   // read file from hdfs
      // filter
      .filter(ApacheAccessLog.isValidateLogLine)
      // parse
      .map(log => ApacheAccessLog.parseLogLine(log))
    // cache data

    import spark.implicits._

    /**
      * 需求一：计算从服务器返回的数据的平均值 最小值 最大值
      *     //1.计算从服务器返回的数据的平均值 最小值 最大值(没说各个ip的组内统计?)
	      The average, min, and max content size of responses returned from the server.
      */
      //使用sparkcore创建rdd做
      //使用log的属性形成rdd
    val  contentSizeRDD= accessLogsRdd.map(log=>log.contentSize)
     //求平均值(sum/count)
    val avg=contentSizeRDD.reduce(_+_)/contentSizeRDD.count()
    //最大值
    val max=contentSizeRDD.max()
    //最小值
    val min=contentSizeRDD.min()
    println(s"平均值:$avg,最大值:$max,最小值:$min")

//rdd转dataframe进行sql查询
   val accessLogsDF= accessLogsRdd.toDF()
    accessLogsDF.createTempView("log")
    //总条数count
    spark.sql("select avg(contentSize) avg,max(contentSize) max,min(contentSize) min  from log ").show()


    /**
      * 需求二： 计算不同状态响应码出现的次数
	        A count of response code's returned.
      */
      //形成rdd
  val responseCodeRDD  =accessLogsRdd.map(log=>log.responseCode)
 val count=responseCodeRDD.map(responseCode=>(responseCode,1))
      .reduceByKey(_+_)


   println(count.collect().foreach(println(_)))

    spark.sql(
      """
        |select
        |responseCode,
        |count(responseCode)
        |from log
        |group by responseCode
      """.stripMargin).show()

    /**
      * 需求三： 访问服务器的IP地址中，访问次数超过20次的ip地址有哪些
	        All IPAddresses that have accessed this server more than N times.
      */

    spark.sql(
      """|select
        |ip.ipAddress,
        |ip.number
        |from
        |(select
        |ipAddress,
        |count(ipAddress) number
        |from log
         |group by ipAddress)  ip
        | where ip.number >20
      """.stripMargin).show()
    /**
      * 需求四：求访问到达的页面次数的topN，前5多
	        The top endpoints requested by count.
      */

  val top5: Array[(String, Int)] =  accessLogsRdd.map(log=>(log.endpoint,1)).
      reduceByKey(_+_)
          .map(i=>(i._2,i._1))
      .top(5)
      .map(_.swap)
  println("top5:==="+top5.mkString("[",",","}"))

    spark.sql(
      """ |select
        |e.endpoint,
        |e.number
        |from
        |(select
        |endpoint,
        |count(endpoint) number
        |from log
        |group by endpoint) e
        | order by number desc limit 5
      """.stripMargin).show()
  }






}

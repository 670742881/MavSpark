package com

import java.net.URLDecoder

import newIpPrise.Test
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

object  Filetest {
  val logger = Logger.getLogger(classOf[LoggerUtil])
  private val serialVersionUID = -4892194648703458595L
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("useridStat")

    val sc = new SparkContext(conf)
    val path=HdfsUtil.getFiles("data/file/")
    println(path)
    val text = sc.textFile("data/file/*")
    // val text = sc.textFile("hdfs://10.10.4.1:8020/ibc/datalogs/apachelogs/2019/01/03/access_logs.1546444800206", 4)
    //val text = sc.textFile("data/test",1)
    //   val count=log.map((_, 1)).filter(_._1.contains("is_bc_review=1")).filter(_._1.contains("en=e_sx"))
    //        .map(_._2).sum()
    val log = text.map(log => {
      var map: Map[String, String] = new HashMap[String, String]
      val splits = log.split("\\^A")
      val ip = splits(0).trim
      val nginxTime = TimeUtil.parseNginxServerTime2Long(splits(1).trim).toString;
      if (nginxTime != "-1") {
        nginxTime.toString
      }
      val requestStr = splits(2)
      val index1 = requestStr.indexOf("?")
      if (index1 > -1) { // 有请求参数的情况下，获取？后面的参数
        val requestBody: String = requestStr.substring(index1 + 1)
        var areaInfo = if (ip.nonEmpty) Test.getInfo(ip) else Array("un", "un", "un")
        val requestParames = requestBody.split("&")
        for (e <- requestParames) {
          val index2 = e.indexOf("=")
          if (index2 < 1) {
            logger.error(e + "次日志无法解析")
          } else {
            var key = "";
            var value = "";
            key = e.substring(0, index2)
            value = URLDecoder.decode(e.substring(index2 + 1).replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8")
            if ("null".equals(value)) value = "无数据" else value
            // value = URLDecoder.decode(e.substring(index2 + 1), EventLogConstants.LOG_PARAM_CHARSET)
            map.+=(key -> value)
          }
        }
        map.+=("ip" -> ip, "s_time" -> nginxTime, "country" -> areaInfo(0), "provence" -> areaInfo(1), "city" -> areaInfo(2),"nginxTime"->nginxTime)
      }
      map

    })
 val uudi= log.filter(log=>log.contains("userId")).map(log=>TimeUtil.parseLong2String(log("nginxTime").trim.toLong,TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT1).substring(0,6)+"\t"+log("userId")).distinct()
  }
}

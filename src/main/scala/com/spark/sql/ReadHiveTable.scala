package com.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadHiveTable {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    spark.read.table("test.dept").show()
  }




}

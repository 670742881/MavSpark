package com.spark.sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveMysqlJoin {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[2]")
        .enableHiveSupport()
      .getOrCreate()

    //读取数据源形成dataframe
    //读取hive表
 val  hiveEmp: DataFrame =spark.read.table("test.emp")
   // val a=spark.table("test.emp")

    val url="jdbc:mysql://localhost:3306/test"
    val table="emp"
    val pro=new Properties()
    pro.put("user","root")
    pro.put("password","123456")
    pro.put("driver","com.mysql.jdbc.Driver")
 //将dataframe结果保存到windows中的mysql
   // hiveEmp.write.mode(SaveMode.Overwrite).jdbc(url,table, pro)
    hiveEmp.write.mode("overwrite").jdbc(url,table, pro)
val hiveDept=spark.read.table("test.dept")
    hiveDept.createTempView("d")
    val mysqlEmp=spark.read.jdbc(
      url,
      table,
      pro
    )
mysqlEmp.createTempView("e")
    import spark.sql
  val res=  sql(
      """
        |select * from e join d on e.deptid=d.deptno group by
      """.stripMargin
    )
    val res1=  sql("select * from e join d on e.deptid=d.deptno group by d.deptno having order by salary")
    res.write.csv("/spark/hiveMsql")
    res1.write.json("/spark/hiveMsql")
  }
}

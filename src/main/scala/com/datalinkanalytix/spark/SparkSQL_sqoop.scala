package com.datalinkanalytix.spark

import org.apache.log4j.{Level, Logger}
import com.datalinkanalytix.spark.SparkSessionWrapper
import com.datalinkanalytix.spark.sparkSQL_02.spark
import org.apache.spark


object SparkSQL_sqoop extends App with SparkSessionWrapper {

  Logger.getLogger("org").setLevel(Level.ERROR)
  //def main(arg: Array[String]): Unit ={
    val spsqoop = spark
      .read
      .format("jdbc")
      .option("url","jdbc:oracle:thin:@//ed01db02-vip:1521/c2yt")

      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/question_tags_10K.csv")
      .toDF("id", "tag")

    spsqoop createOrReplaceTempView("the_tags")

    //list all tables in spark catalog
    spark.catalog.listTables().show()
  //}






}

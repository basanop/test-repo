package com.datalinkanalytix.spark

import org.apache.spark.sql.SparkSession


trait SparkSessionWrapper {

  lazy val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///D:/temp")
    .getOrCreate()

}

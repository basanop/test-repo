package com.datalinkanalytix.spark


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._


object sparkSQL {

  case class ebaysales (Seller:String, Bidder:String, toWeight:Int, Bvolume:Int, Svolume:Int)

  def mapdata(line:String) : ebaysales = {
    val fields = line.split(',')
    val ebay:ebaysales = ebaysales(fields(0), fields(1), fields(2).toInt, fields(3).toInt,fields(4).toInt)
    return ebay
  }
  def main(args: Array[String]){
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///D:/temp")
      .getOrCreate()

    //Seller	Bidder	Weight	Bidder.Volume	Seller.Volume

    val lines = spark.sparkContext.textFile("data/eBayNetwork.csv")
    val ebays = lines.map(mapdata)
    import spark.implicits._
    val schemaebasales = ebays.toDF()
    schemaebasales.printSchema()
    schemaebasales.createOrReplaceTempView("ebays")
    val topsales = spark.sql("SELECT * FROM ebays WHERE Svolume > 2500").show()
    // val results = topsales.collect()
    // results.foreach(println)

    spark.stop()

  }

}

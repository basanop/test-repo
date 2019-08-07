package com.datalinkanalytix.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TypedDataset {

  case class Responses(country:String, age_midpoint:Double, occupation:String, salary_midpoint:Double)

  val AGE_MID_POINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salaryMidpointBucket"

  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///D:/temp")
      .getOrCreate()
    val dataFrameReader = spark.read

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/2016-stack-overflow-survey-responses.csv")

    val responseWithSelectedColumns = responses.select("country","age_midpoint","occupation","salary_midpoint")

    import spark.implicits._

    val TypedDataset = responseWithSelectedColumns.as[Responses]
    println("=== Print out Schema ===")
    TypedDataset.printSchema()
    println("=== Print 20 records of responses table ===")
    TypedDataset.show(20)

    println("=== Print the responses from Afghanistan ===")
    TypedDataset.filter("country = 'Afghanistan'").show()

    println("=== Print the count of occupations ===")
    TypedDataset.groupBy(TypedDataset.col("occupation")).count().show()

    println("=== Print responses with average mid age less than 20 ===")
    TypedDataset.filter("age_midpoint < 20.0").show()

    println("=== Print the result by salary middle point in descending order ===")
    TypedDataset.orderBy(TypedDataset.col(SALARY_MIDPOINT).desc).show()

    spark.catalog.listTables().show()


  }


}

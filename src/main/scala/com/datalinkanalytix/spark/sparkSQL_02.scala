package com.datalinkanalytix.spark
import org.apache.log4j.{Level, Logger}
import com.datalinkanalytix.spark.SparkSessionWrapper

object sparkSQL_02  extends App with SparkSessionWrapper {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val DFtblTags = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  DFtblTags.createOrReplaceTempView("the_tags")

  //list all tables in spark catalog
  spark.catalog.listTables().show()

  //list all tables in spark catalog using spark sql
  spark.sql("show tables").show()

  //Select columns
  spark.sql("select id,tag from the_tags limit 25").show()

  // Filter by column value
  spark
    .sql("select * from the_tags where tag = 'css'")
    .show(10)

  // Count number of rows
  spark
    .sql(
      """select
        |count(*) as css_count
        |from the_tags where tag='css'""".stripMargin)
    .show(10)

  // Using SQL like
  spark
    .sql(
      """select *
        |from the_tags
        |where tag like 's%'""".stripMargin)
    .show(10)

  // SQL where with and clause
  spark
    .sql(
      """select *
        |from the_tags
        |where tag like 's%'
        |and (id = 25 or id = 108)""".stripMargin)
    .show(10)

  // SQL IN clause
  spark
    .sql(
      """select *
        |from the_tags
        |where id in (25, 108)""".stripMargin)
    .show(10)

  // SQL Group By
  spark
    .sql(
      """select tag, count(*) as count
        |from the_tags group by tag""".stripMargin)
    .show(10)

  // SQL Group By with having clause
  spark
    .sql(
      """select tag, count(*) as count
        from the_tags group by tag having count > 5""".stripMargin)
    .show(10)

  // SQL Order by
  spark
    .sql(
      """select tag, count(*) as count
        |from the_tags group by tag having count > 5 order by tag""".stripMargin)
    .show(10)


  // Typed dataframe, filter and temp table
  val dfQuestions = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  dfQuestions.printSchema()

  // cast columns to data types
  val DFQuestions_Tags = dfQuestions.select(
    dfQuestions.col("id").cast("integer"),
    dfQuestions.col("creation_date").cast("timestamp"),
    dfQuestions.col("closed_date").cast("timestamp"),
    dfQuestions.col("deletion_date").cast("date"),
    dfQuestions.col("score").cast("integer"),
    dfQuestions.col("owner_userid").cast("integer"),
    dfQuestions.col("answer_count").cast("integer")
  )

  // filter dataframe
  val DFQuestions_Tags_Subset = DFQuestions_Tags.filter("score > 400 and score < 410").toDF()

  // register temp table
  DFQuestions_Tags_Subset.createOrReplaceTempView("the_questions")

  // SQL Inner Join
  spark
    .sql(
      """select t.*, q.*
        |from the_questions q
        |inner join the_tags t
        |on t.id = q.id""".stripMargin)
    .show(10)

  // SQL Left Outer Join
  spark
    .sql(
      """select t.*, q.*
        |from the_questions q
        |left outer join the_tags t
        |on t.id = q.id""".stripMargin)
    .show(10)

  // SQL Right Outer Join
  spark
    .sql(
      """select t.*, q.*
        |from the_tags t
        |right outer join the_questions q
        |on t.id = q.id""".stripMargin)
    .show(10)

  // SQL Distinct
  spark
    .sql("""select distinct tag from the_tags""".stripMargin)
    .show(10)

  spark.stop()

}

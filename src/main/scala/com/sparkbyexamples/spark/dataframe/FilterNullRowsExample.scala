package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.col

object FilterNullRowsExample extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
   val data = Seq(
    ("James",null,"M"),
    ("Anna","NY","F"),
    ("Julia",null,null)
  )
  import spark.implicits._
  val columns = Seq("name","state","gender")
  val df = data.toDF(columns:_*)

  df.printSchema()
  df.show()

  df.filter("state is NULL").show(false)
  df.filter(df("state").isNull).show(false)
  df.filter(col("state").isNull).show(false)

  df.filter("state is not NULL").show(false)
  df.filter("NOT state is NULL").show(false)
  df.filter(df("state").isNotNull).show(false)

  df.filter("state is NULL AND gender is NULL").show(false)
  df.filter(df("state").isNull && df("gender").isNull).show(false)

  df.createOrReplaceTempView("DATA")
  spark.sql("SELECT * FROM DATA where STATE IS NULL").show(false)
  spark.sql("SELECT * FROM DATA where STATE IS NULL AND GENDER IS NULL").show(false)
  spark.sql("SELECT * FROM DATA where STATE IS NOT NULL").show(false)


}

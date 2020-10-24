package com.sparkbyexamples.spark.dataframe.functions.collection

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CollectListExample extends App {

  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()

  val arrayStructData = Seq(
    Row("James", "Java"), Row("James", "C#"),Row("James", "Python"),
    Row("Michael", "Java"),Row("Michael", "PHP"),Row("Michael", "PHP"),
    Row("Robert", "Java"),Row("Robert", "Java"),Row("Robert", "Java"),
    Row("Washington", null)
  )
  val arrayStructSchema = new StructType().add("name", StringType)
    .add("booksIntersted", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructData),arrayStructSchema)
  df.printSchema()
  df.show(false)

  val df2 = df.groupBy("name").agg(collect_list("booksIntersted")
    .as("booksIntersted"))
  df2.printSchema()
  df2.show(false)

  df.groupBy("name").agg(collect_set("booksIntersted")
    .as("booksIntersted"))
    .show(false)
}
package com.sparkbyexamples.spark.dataframe.functions.collection

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ArrayOfStructType extends App{

  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()

  val arrayStructData = Seq(
    Row("James",List(Row("Java","XX",120),Row("Scala","XA",300))),
    Row("Michael",List(Row("Java","XY",200),Row("Scala","XB",500))),
    Row("Robert",List(Row("Java","XZ",400),Row("Scala","XC",250))),
    Row("Washington",null)
  )

  val arrayStructSchema = new StructType().add("name",StringType)
    .add("booksIntersted",ArrayType(new StructType()
      .add("name",StringType)
      .add("author",StringType)
      .add("pages",IntegerType)))

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructData),arrayStructSchema)
  df.printSchema()
  df.show(false)

  import spark.implicits._
  val df2 = df.select($"name",explode($"booksIntersted"))
  df2.printSchema()
  df2.show(false)

  df2.groupBy($"name").agg(collect_list($"col").as("booksIntersted"))
    .show(false)

}

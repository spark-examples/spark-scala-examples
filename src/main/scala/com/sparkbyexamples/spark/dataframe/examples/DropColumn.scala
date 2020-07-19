package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.col
object DropColumn extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq(
    Row("James","","Smith","36636","NewYork",3100),
    Row("Michael","Rose","","40288","California",4300),
    Row("Robert","","Williams","42114","Florida",1400),
    Row("Maria","Anne","Jones","39192","Florida",5500),
    Row("Jen","Mary","Brown","34561","NewYork",3000)
  )

  val schema = new StructType()
    .add("firstname",StringType)
    .add("middlename",StringType)
    .add("lastname",StringType)
    .add("id",StringType)
    .add("location",StringType)
    .add("salary",IntegerType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),schema)
  df.printSchema()
  df.show(false)

  df.drop(df("firstname"))
    .printSchema()

  df.drop(col("firstname"))
    .printSchema()

  val df2 = df.drop("firstname")
  df2.printSchema()

  df.drop("firstname","middlename","lastname")
    .printSchema()

  val cols = Seq("firstname","middlename","lastname")
  df.drop(cols:_*)
    .printSchema()
}

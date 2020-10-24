package com.sparkbyexamples.spark.dataframe.functions.collection

import org.apache.spark.sql.functions.{array_contains,col}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ArrayContainsExample extends App {

  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()

  val data = Seq(
    Row("James,,Smith",List("Java","Scala","C++"),"CA"),
    Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
    Row("Robert,,Williams",null,"NV")
  )

  val schema = new StructType()
    .add("name",StringType)
    .add("languagesAtSchool", ArrayType(StringType))
    .add("currentState", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),schema)
  df.printSchema()
  df.show(false)

  val df2=df.withColumn("Java Present",
    array_contains(col("languagesAtSchool"),"Java"))
  df2.show(false)

  val df3=df.where(array_contains(col("languagesAtSchool"),"Java"))
  df3.show(false)
}
package com.sparkbyexamples.spark.dataframe.functions.collection

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{explode, flatten}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object ArrayOfArrayType extends App {

  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()

  val arrayArrayData = Seq(
    Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
    Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
    Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
  )

  val arrayArraySchema = new StructType().add("name",StringType)
    .add("subjects",ArrayType(ArrayType(StringType)))

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayArrayData),arrayArraySchema)
  df.printSchema()
  df.show(false)

  import spark.implicits._
  val df2 = df.select($"name",explode($"subjects"))


  df2.printSchema()
  df2.show(false)

  //Convert Array of Array into Single array
  df.select($"name",flatten($"subjects")).show(false)

}

package com.sparkbyexamples.spark.dataframe.functions.collection


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{explode}
import org.apache.spark.sql.types._

object ArrayOfMapType extends App {
  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()

  val arrayMapSchema = new StructType().add("name",StringType)
    .add("properties",
      ArrayType(new MapType(StringType,StringType,true)))

  val arrayMapData = Seq(
    Row("James",List(Map("hair"->"black","eye"->"brown"), Map("height"->"5.9"))),
    Row("Michael",List(Map("hair"->"brown","eye"->"black"),Map("height"->"6"))),
    Row("Robert",List(Map("hair"->"red","eye"->"gray"),Map("height"->"6.3")))
  )

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayMapData),arrayMapSchema)
  df.printSchema()
  df.show(false)

  import spark.implicits._

  val df2 = df.select($"name",explode($"properties"))
  df2.printSchema()
  df2.show(false)
}

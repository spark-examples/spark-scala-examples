package com.sparkbyexamples.spark.dataframe.functions.collection

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object MapToColumn extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val arrayStructureData = Seq(
    Row("James",Map("hair"->"black","eye"->"brown")),
  Row("Michael",Map("hair"->"gray","eye"->"black")),
  Row("Robert",Map("hair"->"brown"))
  )

  val mapType  = DataTypes.createMapType(StringType,StringType)

  val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("property", MapType(StringType,StringType))

  val mapTypeDF = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  mapTypeDF.printSchema()
  mapTypeDF.show(false)

  mapTypeDF.select(col("name"),
    col("property").getItem("hair").as("hair_color"),
    col("property").getItem("eye").as("eye_color"))
    .show(false)

  import spark.implicits._
  val keysDF = mapTypeDF.select(explode(map_keys($"property"))).distinct()
  val keys = keysDF.collect().map(f=>f.get(0))
  val keyCols = keys.map(f=> col("property").getItem(f).as(f.toString))
  mapTypeDF.select(col("name") +: keyCols:_*).show(false)
}
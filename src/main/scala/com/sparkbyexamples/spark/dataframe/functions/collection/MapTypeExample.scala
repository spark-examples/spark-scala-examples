package com.sparkbyexamples.spark.dataframe.functions.collection
import org.apache.spark.sql.functions.{col, explode, lit, map, map_concat, map_from_entries, map_keys, map_values}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object MapTypeExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  //Creating DF with MapType
  val arrayStructureData = Seq(
    Row("James",List(Row("Newark","NY"),Row("Brooklyn","NY")),
      Map("hair"->"black","eye"->"brown"), Map("height"->"5.9")),
    Row("Michael",List(Row("SanJose","CA"),Row("Sandiago","CA")),
      Map("hair"->"brown","eye"->"black"),Map("height"->"6")),
    Row("Robert",List(Row("LasVegas","NV")),
      Map("hair"->"red","eye"->"gray"),Map("height"->"6.3")),
    Row("Maria",null,Map("hair"->"blond","eye"->"red"),
      Map("height"->"5.6")),
    Row("Jen",List(Row("LAX","CA"),Row("Orange","CA")),
      Map("white"->"black","eye"->"black"),Map("height"->"5.2"))
  )


  val mapType  = DataTypes.createMapType(StringType,StringType)

  val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("addresses", ArrayType(new StructType()
      .add("city",StringType)
      .add("state",StringType)))
    .add("properties", mapType)
    .add("secondProp", MapType(StringType,StringType))

  val mapTypeDF = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  mapTypeDF.printSchema()
  mapTypeDF.show(false)

  mapTypeDF.select(col("name"),map_keys(col("properties"))).show(false)
  mapTypeDF.select(col("name"),map_values(col("properties"))).show(false)
  mapTypeDF.select(col("name"),map_concat(col("properties"),col("secondProp"))).show(false)



}

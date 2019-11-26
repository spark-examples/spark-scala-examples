package com.sparkbyexamples.spark.dataframe.functions.collection

import org.apache.spark.sql.functions.{col, explode, lit, map, map_concat, map_from_entries, map_keys, map_values}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}

import scala.collection.mutable

object MapFunctions extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  import spark.implicits._

  val structureData = Seq(
    Row("36636","Finance",Row(3000,"USA")),
    Row("40288","Finance",Row(5000,"IND")),
    Row("42114","Sales",Row(3900,"USA")),
    Row("39192","Marketing",Row(2500,"CAN")),
    Row("34534","Sales",Row(6500,"USA"))
  )

  val structureSchema = new StructType()
    .add("id",StringType)
    .add("dept",StringType)
    .add("properties",new StructType()
      .add("salary",IntegerType)
      .add("location",StringType)
    )

  var df = spark.createDataFrame(
    spark.sparkContext.parallelize(structureData),structureSchema)
  df.printSchema()
  df.show(false)

  // Convert to Map
  val index = df.schema.fieldIndex("properties")
  val propSchema = df.schema(index).dataType.asInstanceOf[StructType]
  var columns = mutable.LinkedHashSet[Column]()
  propSchema.fields.foreach(field =>{
    columns.add(lit(field.name))
    columns.add(col("properties." + field.name))
  })

  df = df.withColumn("propertiesMap",map(columns.toSeq:_*))
  df = df.drop("properties")
  df.printSchema()
  df.show(false)

  //Retrieve all keys from a Map
  val keys = df.select(explode(map_keys($"propertiesMap"))).as[String].distinct.collect
  print(keys.mkString(","))

  // map_keys
  df.select(col("id"),map_keys(col("propertiesMap")))
    .show(false)

  //map_values
  df.select(col("id"),map_values(col("propertiesMap")))
    .show(false)

  //Creating DF with MapType
  val arrayStructureData = Seq(
    Row("James",List(Row("Newark","NY"),Row("Brooklyn","NY")),Map("hair"->"black","eye"->"brown"), Map("height"->"5.9")),
  Row("Michael",List(Row("SanJose","CA"),Row("Sandiago","CA")),Map("hair"->"brown","eye"->"black"),Map("height"->"6")),
  Row("Robert",List(Row("LasVegas","NV")),Map("hair"->"red","eye"->"gray"),Map("height"->"6.3")),
  Row("Maria",null,Map("hair"->"blond","eye"->"red"),Map("height"->"5.6")),
  Row("Jen",List(Row("LAX","CA"),Row("Orange","CA")),Map("white"->"black","eye"->"black"),Map("height"->"5.2"))
  )

  val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("addresses", ArrayType(new StructType()
      .add("city",StringType)
      .add("state",StringType)))
    .add("properties", MapType(StringType,StringType))
    .add("secondProp", MapType(StringType,StringType))

  val concatDF = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  concatDF.printSchema()
  concatDF.show()

  concatDF.withColumn("mapConcat",map_concat(col("properties"),col("secondProp")))
    .select("name","mapConcat")
    .show(false)

  concatDF.withColumn("mapFromEntries",map_from_entries(col("addresses")))
    .select("name","mapFromEntries")
    .show(false)
}

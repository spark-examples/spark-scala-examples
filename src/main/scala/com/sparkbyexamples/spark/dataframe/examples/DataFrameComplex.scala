package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFrameComplex extends App {


  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val structureData = Seq(
    Row(Row("James","","Smith"),"36636","NewYork",3100, List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
    Row(Row("Michael","Rose",""),"40288","California",4300,List("Python","PHP"),Map("hair"->"black","eye"->"brown")),
    Row(Row("Robert","","Williams"),"42114","Florida",1400,List("C++","C#"),Map("hair"->"black","eye"->"brown")),
    Row(Row("Maria","Anne","Jones"),"39192","Florida",5500,List("Python","Scala"),Map("hair"->"black","eye"->"brown")),
    Row(Row("Jen","Mary","Brown"),"34561","NewYork",3000,List("R","Scala"),Map("hair"->"black","eye"->"brown"))
  )

  val structureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("id",StringType)
    .add("location",StringType)
    .add("salary",IntegerType)
    .add("languagesKnown",ArrayType(StringType))
    .add("properties",MapType(StringType,StringType))


  val df2 = spark.createDataFrame(spark.sparkContext.parallelize(structureData),structureSchema)
  df2.printSchema()
  df2.show(false)

}

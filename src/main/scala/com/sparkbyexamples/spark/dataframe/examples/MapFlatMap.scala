package com.sparkbyexamples.spark.dataframe.examples

import com.sparkbyexamples.spark.rdd.functions.FlatMapExample.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object MapFlatMap extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq("Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s")

  import spark.sqlContext.implicits._
  val df = data.toDF("data")
  df.show(false)

  //Map Transformation
  val mapDF=df.map(fun=> {
    fun.getString(0).split(" ")
  })
  mapDF.show(false)

  //Flat Map Transformation
  val flatMapDF=df.flatMap(fun=>
    {
      fun.getString(0).split(" ")
    })
  flatMapDF.show()

  val arrayStructureData = Seq(
    Row("James,,Smith",List("Java","Scala","C++"),"CA"),
    Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
    Row("Robert,,Williams",List("CSharp","VB","R"),"NV")
  )

  val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("languagesAtSchool", ArrayType(StringType))
    .add("currentState", StringType)

  val df1 = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)


  //flatMap() Usage
  val df2=df1.flatMap(f => {
    val lang=f.getSeq[String](1)
    lang.map((f.getString(0),_,f.getString(2)))
  })

  val df3=df2.toDF("Name","language","State")
  df3.show(false)


}

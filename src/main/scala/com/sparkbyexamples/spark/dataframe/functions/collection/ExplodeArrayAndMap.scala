package com.sparkbyexamples.spark.dataframe.functions.collection

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}

object ExplodeArrayAndMap extends App{

    val spark = SparkSession.builder().appName("SparkByExamples.com")
      .master("local[1]")
      .getOrCreate()

    //Array

    val arrayData = Seq(
      Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
    Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
    Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
    Row("Washington",null,null),
    Row("Jeferson",List(),Map())
    )

    val arraySchema = new StructType()
      .add("name",StringType)
      .add("knownLanguages", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
    df.printSchema()
    df.show(false)

    import spark.implicits._
    //explode
    df.select($"name",explode($"knownLanguages"))
      .show(false)

    //explode_outer
    df.select($"name",explode_outer($"knownLanguages"))
      .show(false)

    //posexplode
    df.select($"name",posexplode($"knownLanguages"))
      .show(false)

    //posexplode_outer
    df.select($"name",posexplode_outer($"knownLanguages"))
      .show(false)

    // Map

    //explode
    df.select($"name",explode($"properties"))
      .show(false)
    //explode_outer
    df.select($"name",explode_outer($"properties"))
      .show(false)
    //posexplode
    df.select($"name",posexplode($"properties"))
      .show(false)

    //posexplode_outer
    df.select($"name",posexplode_outer($"properties"))
      .show(false)

    // How to explode Array of Structure
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

    val df3 = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructData),arrayStructSchema)
    df3.printSchema()
    df3.show()

    df3.select($"name",explode($"booksIntersted")).show(false)

    // How to explode Array of Map
    val arrayMapSchema = new StructType().add("name",StringType)
      .add("properties",ArrayType(new MapType(StringType,StringType,true)))

    val arrayMapData = Seq(
      Row("James",List(Map("hair"->"black","eye"->"brown"), Map("height"->"5.9"))),
    Row("Michael",List(Map("hair"->"brown","eye"->"black"),Map("height"->"6"))),
    Row("Robert",List(Map("hair"->"red","eye"->"gray"),Map("height"->"6.3")))
    )

    val df5 = spark.createDataFrame(spark.sparkContext.parallelize(arrayMapData),arrayMapSchema)
    df5.printSchema()
    df5.show()

    df5.select($"name",explode($"properties"))
      .show(false)

    // How to explode Array of Array

    val arrayArrayData = Seq(
      Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
      Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
      Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
    )

    val arrayArraySchema = new StructType().add("name",StringType)
      .add("subjects",ArrayType(ArrayType(StringType)))

    val df4 = spark.createDataFrame(spark.sparkContext.parallelize(arrayArrayData),arrayArraySchema)
    df4.printSchema()
    df4.show()

    df4.select($"name",explode($"subjects"))
      .select($"name",explode($"col"))
      .show(false)

    //Convert Array of Array into Single array
    df4.select($"name",flatten($"subjects")).show(false)


}

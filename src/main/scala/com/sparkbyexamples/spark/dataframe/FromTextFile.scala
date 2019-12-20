package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FromTextFile {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    //returns DataFrame
    val df:DataFrame = spark.read.text("src/main/resources/csv/text01.txt")
    df.printSchema()
    df.show(false)

    //converting to columns by splitting
    import spark.implicits._
    val df2 = df.map(f=>{
      val elements = f.getString(0).split(",")
      (elements(0),elements(1))
    })

    df2.printSchema()
    df2.show(false)

    // returns Dataset[String]
    val ds:Dataset[String] = spark.read.textFile("src/main/resources/csv/text01.txt")
    ds.printSchema()
    ds.show(false)

    //converting to columns by splitting
    import spark.implicits._
    val ds2 = ds.map(f=> {
     val elements = f.split(",")
      (elements(0),elements(1))
    })

    ds2.printSchema()
    ds2.show(false)
  }
}

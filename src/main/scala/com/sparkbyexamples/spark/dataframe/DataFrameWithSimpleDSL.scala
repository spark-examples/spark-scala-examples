package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameWithSimpleDSL {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val filePath = "C://000_Projects/opt/BigData/zipcodes.csv"

    var df:DataFrame = spark.read.option("header","true").csv(filePath)
    df.printSchema()

    // Where
    df.select("*").where(df("RecordNumber") < 10).show()
    //Filter
    df.filter(df("State")==="PR").select("State").show()
    //Distinct
    df.select(df("State")).distinct().show()
    //Count
    println("Number of records"+df.count())

    //When Otherwise
    //df.select(df("State"), case df("State") when "PR" then "PR123"

    // where with and and or conditions
    df.where(df("State") === "PR" && df("City").contains("DEL")).show()

    //Order or Sort by
    df.orderBy(df("RecordNumber").desc, df("State").asc).show()


  }
}

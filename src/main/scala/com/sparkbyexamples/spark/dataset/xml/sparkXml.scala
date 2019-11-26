package com.sparkbyexamples.spark.dataset.xml

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{SQLContext, SparkSession}

object sparkXml {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.
      builder.master("local[*]")
      //.config("spark.debug.maxToStringFields", "100")
      .appName("Insight Application Big Data")
      .getOrCreate()

    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "row")
      .load("src/main/resources/input.xml")
    df.createOrReplaceTempView("categ_entry")

    df.printSchema()
    spark.sql("Select c26['_VALUE'] as value, c26['_m'] as option from categ_entry").show(false)

     val df2 = df.withColumn("c26Struct",explode(df("c26")))
     df2.select(col("c26Struct._VALUE").alias("value"),col("c26Struct._m").alias("option") ).show(false)



  }
}

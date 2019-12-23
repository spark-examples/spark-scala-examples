package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReadJsonFromString extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //Read JSON string from text file
  val dfFromText:DataFrame = spark.read.text("src/main/resources/simple_zipcodes.txt")
  dfFromText.printSchema()

  val schema = new StructType()
    .add("Zipcode", StringType, true)
    .add("ZipCodeType", StringType, true)
    .add("City", StringType, true)
    .add("State", StringType, true)

  val dfJSON = dfFromText.withColumn("jsonData",from_json(col("value"),schema))
    .select("jsonData.*")
  dfJSON.printSchema()
  dfJSON.show(false)

  //alternatively using select
  val dfJSON2 = dfFromText.select(from_json(col("value"), schema).as("jsonData"))
    .select("jsonData.*")

  //Read JSON string from CSV file
  val dfFromCSV:DataFrame = spark.read.option("header",true)
     .csv("src/main/resources/simple_zipcodes.csv")
  dfFromCSV.printSchema()
  dfFromCSV.show(false)

  val dfFromCSVJSON =  dfFromCSV.select(col("Id"),
    from_json(col("JsonValue"),schema).as("jsonData"))
      .select("Id","jsonData.*")
  dfFromCSVJSON.printSchema()
  dfFromCSVJSON.show(false)

  //Read json from string
  import spark.implicits._
  val jsonStr = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
  val df = spark.read.json(Seq(jsonStr).toDS())
  df.show(false)

  // from RDD[String]
  // deprecated
  val rdd = spark.sparkContext.parallelize(
    """ {"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"} """ :: Nil)
  val df2 = spark.read.json(rdd)
  df2.show()

}

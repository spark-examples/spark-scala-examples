package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataFrameFromCSVFile {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //spark read csv file
    val df = spark.read.csv("src/main/resources/zipcodes.csv")
    df.show()
    df.printSchema()

    //read csv with options
    val df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("src/main/resources/zipcodes.csv")
    df2.show()
    df2.printSchema()

    //read with custom schema
    import org.apache.spark.sql.types._
    val schema = new StructType()
      .add("RecordNumber",IntegerType,true)
      .add("Zipcode",IntegerType,true)
      .add("ZipCodeType",StringType,true)
      .add("City",StringType,true)
      .add("State",StringType,true)
      .add("LocationType",StringType,true)
      .add("Lat",DoubleType,true)
      .add("Long",DoubleType,true)
      .add("Xaxis",DoubleType,true)
      .add("Yaxis",DoubleType,true)
      .add("Zaxis",DoubleType,true)
      .add("WorldRegion",StringType,true)
      .add("Country",StringType,true)
      .add("LocationText",StringType,true)
      .add("Location",StringType,true)
      .add("Decommisioned",BooleanType,true)
      .add("TaxReturnsFiled",IntegerType,true)
      .add("EstimatedPopulation",IntegerType,true)
      .add("TotalWages",IntegerType,true)
      .add("Notes",StringType,true)

    //Write dataframe back to csv file
    val df_with_schema = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load("src/main/resources/zipcodes.csv")

    df_with_schema.printSchema()
    df_with_schema.show(false)


    //Write a csv file
    df_with_schema.write.mode(SaveMode.Overwrite)
      .csv("c:/tmp/spark_output/zipcodes")

  }
}

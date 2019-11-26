package com.sparkbyexamples.spark.dataframe.xml

import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}

object PersonsXML {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    /*
    Read XML File
     */
    val df = spark.read
      .format("xml")
      .option("rowTag", "person")
      .load("src/main/resources/persons.xml")

    df.printSchema()
    df.show()
    
    val schema = new StructType()
      .add("_id",StringType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",StringType)
      .add("dob_month",StringType)
      .add("gender",StringType)
      .add("salary",StringType)

    val df2 = spark.read
      .format("xml")
      .option("rowTag", "person")
      .schema(schema)
      .load("src/main/resources/persons.xml")

    df2.write
      .format("com.databricks.spark.xml")
      .option("rootTag", "persons")
      .option("rowTag", "person")
      .save("src/main/resources/persons_new.xml")

  }
}



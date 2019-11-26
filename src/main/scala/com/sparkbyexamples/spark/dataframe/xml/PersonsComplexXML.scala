package com.sparkbyexamples.spark.dataframe.xml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}

object PersonsComplexXML {

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
      .load("src/main/resources/persons_complex.xml")

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
      .add("addresses",  new StructType()
        .add("address",ArrayType(
          new StructType()
            .add("_type",StringType)
            .add("addressLine",StringType)
            .add("city",StringType)
            .add("state",StringType)
          )
        )
      )

    val df2 = spark.read
      .format("xml")
      .option("rowTag", "person")
      .schema(schema)
      .load("src/main/resources/persons.xml")

//    df.foreach(row=>{
//      println("ID:"+row.getAs("_id") )
//      println("ID:"+row(0))
//      println("ID:"+row.get(0))
//      println(row.getAs("addresses"))
//     // println("ID:"+row.getString(0))
//    })
//
    df2.write
      .format("com.databricks.spark.xml")
      .option("rootTag", "persons")
      .option("rowTag", "person")
      .save("src/main/resources/persons_new.xml")

  }
}



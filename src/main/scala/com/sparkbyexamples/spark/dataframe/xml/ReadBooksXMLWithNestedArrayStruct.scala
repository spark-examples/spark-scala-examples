package com.sparkbyexamples.spark.dataframe.xml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

object ReadBooksXMLWithNestedArrayStruct {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val customSchema = StructType(Array(
      StructField("_id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType ,nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("otherInfo",StructType(Array(
        StructField("pagesCount", StringType, nullable = true),
        StructField("language", StringType, nullable = true),
        StructField("country", StringType, nullable = true),
        StructField("address", StructType(Array(
          StructField("addressline1", StringType, nullable = true),
          StructField("city", StringType, nullable = true),
          StructField("state", StringType, nullable = true)
          ))
        ))
      )),
      StructField("stores",StructType(Array(
        StructField("store",ArrayType(
          StructType(Array(
            StructField("location",StringType,true),
            StructField("name",StringType,true)
          ))
        ))
      )))
    ))

    val df = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .schema(customSchema)
      .load("src/main/resources/books_withnested_array.xml")

    df.printSchema()
    df.show()

    df.foreach(row=>{
      println(""+row.getAs("author")+","+row.getAs("_id"))
      println(row.getStruct(4).getAs("country"))
      println(row.getStruct(4).getClass)
      val arr = row.getStruct(7).getList(0)
      for (i<-0 to arr.size-1){
        val b = arr.get(i).asInstanceOf[GenericRowWithSchema]
        println(""+b.getAs("name") +","+b.getAs("location"))
      }
    })

  }
}


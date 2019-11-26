package com.sparkbyexamples.spark.dataframe.xml

import com.sparkbyexamples.spark.beans.BooksWithArray
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

object ReadBooksXMLWithNestedArray {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val df = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
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


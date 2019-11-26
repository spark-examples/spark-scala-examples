package com.sparkbyexamples.spark.dataset.xml



import com.sparkbyexamples.spark.beans.Books
import org.apache.spark.sql.{Encoders, SparkSession, functions}

object ReadBooksXMLWithNestedArrayDSL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._
    val xmlDF = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .load("src/main/resources/books_withnested_array.xml")

    xmlDF.printSchema()
    println(xmlDF.count())

    xmlDF.show()

    xmlDF.select(xmlDF("title"),xmlDF("price")*100).show()

    xmlDF.select("author").show()


    xmlDF.select("stores").show()

    xmlDF.withColumn("store", functions.explode(xmlDF("stores.store"))).show()

    val df = xmlDF.withColumn("store", functions.explode(xmlDF("stores.store")))
      .select("_id","author","stores.country","store.name")

    val storeDF = xmlDF.select("stores.store")
    storeDF.printSchema()

    df.foreach(f=>{
      println(f.getAs("_id"))
    })







  }
}



package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CreateEmptyDatasetExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  import spark.implicits._

  val schema = StructType(
    StructField("firstName", StringType, true) ::
      StructField("lastName", IntegerType, false) ::
      StructField("middleName", IntegerType, false) :: Nil)

  val colSeq = Seq("firstName","lastName","middleName")

  case class Name(firstName: String, lastName: String, middleName:String)

  spark.createDataset(Seq.empty[Name])
  spark.createDataset(Seq.empty[(String,String,String)])
  spark.createDataset(spark.sparkContext.emptyRDD[Name])
  Seq.empty[(String,String,String)].toDS()
  Seq.empty[Name].toDS()
  spark.emptyDataset[Name]
}
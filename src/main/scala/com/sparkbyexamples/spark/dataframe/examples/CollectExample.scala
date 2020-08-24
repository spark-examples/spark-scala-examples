package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object CollectExample extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
    Row(Row("Michael ","Rose",""),"40288","M",4000),
    Row(Row("Robert ","","Williams"),"42114","M",4000),
    Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )

  val schema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("id",StringType)
    .add("gender",StringType)
    .add("salary",IntegerType)

  val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
  df.printSchema()
  df.show(false)

  val colList = df.collectAsList()
  val colData = df.collect()

  colData.foreach(row=>
  {
    val salary = row.getInt(3)//Index starts from zero
    println(salary)
  })

  //Retrieving data from Struct column
  colData.foreach(row=>
  {
    val salary = row.getInt(3)
    val fullName:Row = row.getStruct(0) //Index starts from zero
    val firstName = fullName.getString(0)//In struct row, again index starts from zero
    val middleName = fullName.get(1).toString
    val lastName = fullName.getAs[String]("lastname")
    println(firstName+","+middleName+","+lastName+","+salary)
  })

}

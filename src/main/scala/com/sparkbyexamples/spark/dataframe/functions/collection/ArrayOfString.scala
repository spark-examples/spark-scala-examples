package com.sparkbyexamples.spark.dataframe.functions.collection

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions.{col,concat_ws}

object ArrayOfString extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val arrayStructureData = Seq(
    Row("James,,Smith",List("Java","Scala","C++"),"CA"),
    Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
    Row("Robert,,Williams",List("CSharp","VB"),"NV")
  )

  val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("languagesAtSchool", ArrayType(StringType))
    .add("currentState", StringType)


  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  df.printSchema()
  df.show()

  val df2 = df.withColumn("languagesAtSchool",
    concat_ws(",",col("languagesAtSchool")))
  df2.printSchema()
  df2.show()

  import spark.implicits._
  val df3 = df.map(f=>{
    val name = f.getString(0)
    val lang = f.getList(1).toArray.mkString(",")
    (name,lang,f.getString(2))
  })

  df3.toDF("Name","Languages","currentState")
     .show(false)

  df.createOrReplaceTempView("ARRAY_STRING")
  spark.sql("select name, concat_ws(',',languagesAtSchool) as languagesAtSchool," +
    " currentState from ARRAY_STRING")
    .show(false)
}

//package com.sparkbyexamples.spark.dataframe.functions
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.types.{StringType, StructType}
//
//object from_json {
//  def main(args:Array[String]):Unit= {
//
//    val spark: SparkSession = SparkSession.builder()
//      .master("local[1]")
//      .appName("SparkByExample")
//      .getOrCreate()
//
//
//    val data = Seq(("1","{\"name\":\"Anne\",\"Age\":\"12\",\"country\":\"Denmark\"}"),
//      ("2","{\"name\":\"Zen\",\"Age\":\"24\"}"),
//      ("3","{\"name\":\"Fred\",\"Age\":\"20\",\"country\":\"France\"}"),
//      ("4","{\"name\":\"Mona\",\"Age\":\"18\",\"country\":\"Denmark\"}")
//    )
//
//    import spark.sqlContext.implicits._
//    val df = data.toDF("ID","details_Json")
//
//    val schema = (new StructType()).add("name",StringType,true)
//      .add("Age",StringType,true)
//      .add("country",StringType,true)
//
//    val df2 = df.withColumn("details_Struct", from_json($"details_Json", schema))
//        .withColumn("country",col("details_Struct").getField("country"))
//        .filter(col("country").equalTo("Denmark"))
//
//
//    df2.printSchema()
//    df2.show(false)
//  }
//}

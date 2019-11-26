package com.sparkbyexamples.spark.dataframe.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object WhenOtherwise {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val data = List(("James ","","Smith","36636","M",60000),
        ("Michael ","Rose","","40288","M",70000),
        ("Robert ","","Williams","42114","",400000),
        ("Maria ","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0))

    val cols = Seq("first_name","middle_name","last_name","dob","gender","salary")

    val df = spark.createDataFrame(data).toDF(cols:_*)

    val df2 = df.withColumn("gender", when(col("gender") === "M","Male")
      .when(col("gender") === "F","Female")
      .otherwise("Unknown"))


    val df3 = df.withColumn("gender",
      expr("case when gender = 'M' then 'Male' " +
                       "when gender = 'F' then 'Female' " +
                       "else 'Unknown' end"))

    val df4 = df.select(col("*"), when(col("gender") === "M","Male")
      .when(col("gender") === "F","Female")
      .otherwise("Unknown").alias("new_gender"))

    val df5 = df.select(col("*"),
      expr("case when gender = 'M' then 'Male' " +
                       "when gender = 'F' then 'Female' " +
                       "else 'Unknown' end").alias("new_gender"))

    val dataDF = Seq(
      (66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4"
      )).toDF("id", "code", "amt")

    df2.show()
    df3.show()
    df4.show()
    df5.show()
    dataDF.show()

    dataDF.withColumn("new_column",
       when(col("code") === "a" || col("code") === "d", "A")
      .when(col("code") === "b" and col("amt") === "4", "B")
      .otherwise("A1"))
      .show()

    //alternatively, we can also use "and" "or" operators
    dataDF.withColumn("new_column",
      when(col("code") === "a" or col("code") === "d", "A")
        .when(col("code") === "b" and col("amt") === "4", "B")
        .otherwise("A1"))
      .show()

  }
}

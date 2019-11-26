package com.sparkbyexamples.spark.dataframe.functions

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

class AnotherExample {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    /**
      * Simple using columns list
      */
    val data = Seq(("James ","","Smith","2018","01","M",3000),
      ("Michael ","Rose","","2010","03","M",4000),
      ("Robert ","","Williams","2010","03","M",4000),
      ("Maria ","Anne","Jones","2005","05","F",4000),
      ("Jen","Mary","Brown","2010","07","",-1)
    )

    val columns = Seq("firstname","middlename","lastname","dob_year","dob_month","gender","salary")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)

    /**
      * schema using Row data
      */
    val data3 = Seq(Row("James ","","Smith","36636","M",3000),
      Row("Michael ","Rose","","40288","M",4000),
      Row("Robert ","","Williams","42114","M",4000),
      Row("Maria ","Anne","Jones","39192","F",4000),
      Row("Jen","Mary","Brown","","F",-1)
    )

    val schema3 = new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val df3 = spark.createDataFrame(spark.sparkContext.parallelize(data3),schema3)

    /**
     * nested structure schema
     */
    val data4 = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
      Row(Row("Michael ","Rose",""),"40288","M",4000),
      Row(Row("Robert ","","Williams"),"42114","M",4000),
      Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )

    val schema4 = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val df4 = spark.createDataFrame(spark.sparkContext.parallelize(data4),schema4)





  }
}

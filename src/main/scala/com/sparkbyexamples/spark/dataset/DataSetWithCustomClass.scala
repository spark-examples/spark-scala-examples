package com.sparkbyexamples.spark.dataset

import org.apache.spark.sql.SparkSession

class Test(field1:String,field2:String,field3:String) extends Serializable{


}

object TestEncoders {
  implicit def testEncoder: org.apache.spark.sql.Encoder[Test] =
    org.apache.spark.sql.Encoders.kryo[Test]
}
object DataSetWithCustomClass {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val test:Test = new Test("Field1","Field2","Field3")

    import spark.sqlContext.implicits._
    import org.apache.spark.sql.Encoders
    import TestEncoders._
   // implicit val encoder = Encoders.bean[Test](classOf[Test])

    val data = Seq(test)
    val rdd = spark.sparkContext.parallelize(data)
    val ds = spark.createDataset(rdd)

    val ds2 = ds.selectExpr("CAST(value AS String)")
      .as[(String)]


    ds.printSchema()
    ds2.show(false)
  }
}

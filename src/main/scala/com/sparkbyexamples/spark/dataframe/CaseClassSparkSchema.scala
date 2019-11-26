package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

object CaseClassSparkSchema extends App{

  case class Name(first:String,last:String,middle:String)
  case class Employee(fullName:Name,age:Integer,gender:String)

  val encoderSchema = Encoders.product[Employee].schema
  encoderSchema.printTreeString()

  import org.apache.spark.sql.catalyst.ScalaReflection
  val schema = ScalaReflection.schemaFor[Employee].dataType.asInstanceOf[StructType]

}

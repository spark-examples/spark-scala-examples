package com.sparkbyexamples.spark.dataframe.xml.xstream

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.io.xml.DomDriver
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object WriteXML {
  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExample")
      .getOrCreate()

    val sc = spark.sparkContext

    val data = Seq(Row("1",Row("James ","","Smith"),"36636","M","3000"),
      Row("2",Row("Michael ","Rose",""),"40288","M","4000"),
      Row("3",Row("Robert ","","Williams"),"42114","M","4000"),
      Row("4",Row("Maria ","Anne","Jones"),"39192","F","4000"),
      Row("5",Row("Jen","Mary","Brown"),"","F","-1")
    )

    val schema = new StructType()
      .add("id",StringType)
      .add("name",new StructType()
        .add("firstName",StringType)
        .add("middleName",StringType)
        .add("lastName",StringType))
      .add("ssn",StringType)
      .add("gender",StringType)
      .add("salary",StringType)

    case class Name(firstName:String,middleName:String,lastName:String)
    case class Person(id:String,name:Name,ssn:String,gender:String,salary:String)
    import spark.sqlContext.implicits._

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)//.as[Person]

    val ds = df.mapPartitions(part=>{
      val xstream = new XStream(new DomDriver)
      val data = part.map(ite=>{
        val nameRow:Row = ite.getAs[Row]("name")
        val name= Name(nameRow.getAs("firstName"),nameRow.getAs("firstName"),nameRow.getAs("firstName"))
        val person = Person(ite.getAs("id"),name,ite.getAs("ssn"),ite.getAs("gender"),ite.getAs("salary"))
        //xstream.aliasType("Person",Class[String])
        val xmlString = xstream.toXML(person)
        xmlString
      })
      data
    })

    ds.write.text("c:/tmp/xstream.xml")

//    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data),schema).as[Person]
//
//    val ds2 = df2.mapPartitions(part=>{
//      val xstream = new XStream(new DomDriver)
//      val person = part.map(ite=>{
//        val xmlString = xstream.toXML(person)
//        xmlString
//      })
//      person
//    })
//    ds2.write.text("c:/tmp/xstream_2.xml")
  }
}

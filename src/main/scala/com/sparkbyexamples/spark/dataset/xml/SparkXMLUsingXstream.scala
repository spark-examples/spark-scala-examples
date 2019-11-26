package com.sparkbyexamples.spark.dataframe.xml

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.io.xml.DomDriver
import org.apache.spark.sql.SparkSession

case class Animal(cri:String,taille:Int)

object SparkXMLUsingXStream{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
    builder.master ("local[*]")
    .appName ("sparkbyexamples.com")
    .getOrCreate ()

    var animal:Animal = Animal("Rugissement",150)
    val xstream1 = new XStream(new DomDriver())
    xstream1.alias("testAni",classOf[Animal])
    xstream1.aliasField("cricri",classOf[Animal],"cri")
    val xmlString = Seq(xstream1.toXML(animal))

    import spark.implicits._
    val newDf = xmlString.toDF()
    newDf.show(false)
  }
}

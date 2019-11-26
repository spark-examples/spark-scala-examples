package com.sparkbyexamples.spark.rdd.xml

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.sql.SparkSession

import scala.xml.XML


object XmlRecordReader {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.appName("XmlRecordReader").master("local").getOrCreate
    val javaSparkContext = new JavaSparkContext(sparkSession.sparkContext)
    val configuration = new Configuration
    configuration.set("xmlinput.start", "<Rec>")
    configuration.set("xmlinput.end", "</Rec>")
    configuration.set("mapreduce.input.fileinputformat.inputdir", "src/main/resources/records.xml")
    val javaPairRDD = javaSparkContext.newAPIHadoopRDD(configuration, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])
    javaPairRDD.foreach(new VoidFunction[Tuple2[LongWritable, Text]]() {
      @throws[Exception]
      override def call(tuple: Tuple2[LongWritable, Text]): Unit = { // TODO Auto-generated method stub

        val xml = XML.loadString(tuple._2.toString)
        val forecast = (xml \ "Name") text

        println("forecast" + forecast)

      }
    })
  }
}


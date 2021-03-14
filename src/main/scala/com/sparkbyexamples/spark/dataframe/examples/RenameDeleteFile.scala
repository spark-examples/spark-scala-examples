package com.sparkbyexamples.spark.dataframe.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

object RenameDeleteFile extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  //Create Hadoop Configuration from Spark
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  val srcPath=new Path("/tmp/address_rename_merged.csv")
  val destPath= new Path("/tmp/address_merged.csv")

  //Rename a File
  if(fs.exists(srcPath) && fs.isFile(srcPath))
    fs.rename(srcPath,destPath)

  //Alternatively, you can also create Hadoop configuration
  val hadoopConfig = new Configuration()
  val hdfs = FileSystem.get(hadoopConfig)
  if(hdfs.isFile(srcPath))
    hdfs.rename(srcPath,destPath)


  //Delete a File
  if(hdfs.isDirectory(srcPath))
    hdfs.delete(new Path("/tmp/.address_merged2.csv.crc"),true)

  import scala.sys.process._
  //Delete a File
  s"hdfs dfs -rm /tmp/.address_merged2.csv.crc" !

  //Delete a Directory
  s"hdfs dfs -rm -r /tmp/.address_merged2.csv.crc" !


}

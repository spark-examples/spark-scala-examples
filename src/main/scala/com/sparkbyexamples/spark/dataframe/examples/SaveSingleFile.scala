package com.sparkbyexamples.spark.dataframe.examples

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveSingleFile extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val df = spark.read.option("header",true)
    .csv("src/main/resources/address.csv")
  df.repartition(1)
    .write.mode(SaveMode.Overwrite).csv("/tmp/address")


  val hadoopConfig = new Configuration()
  val hdfs = FileSystem.get(hadoopConfig)

  val srcPath=new Path("/tmp/address")
  val destPath= new Path("/tmp/address_merged.csv")
  val srcFile=FileUtil.listFiles(new File("c:/tmp/address"))
    .filterNot(f=>f.getPath.endsWith(".csv"))(0)
  //Copy the CSV file outside of Directory and rename
  FileUtil.copy(srcFile,hdfs,destPath,true,hadoopConfig)
  //Remove Directory created by df.write()
  hdfs.delete(srcPath,true)
  //Removes CRC File
  hdfs.delete(new Path("/tmp/.address_merged.csv.crc"),true)

  // Merge Using Haddop API
  df.repartition(1).write.mode(SaveMode.Overwrite)
    .csv("/tmp/address-tmp")
  val srcFilePath=new Path("/tmp/address-tmp")
  val destFilePath= new Path("/tmp/address_merged2.csv")
  FileUtil.copyMerge(hdfs, srcFilePath, hdfs, destFilePath, true, hadoopConfig, null)
  //Remove hidden CRC file if not needed.
  hdfs.delete(new Path("/tmp/.address_merged2.csv.crc"),true)

}

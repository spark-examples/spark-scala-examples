
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.clustering.{KMeans}
import org.apache.spark.mllib.linalg.Vectors

object kmeansExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("KMeansExample")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
    val data = sc.textFile("src/main/resources/newData.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()


    println("***********************************************************************************************")
    println("***********************************************************************************************")

    println("Hello, Spark! Just read the file!")

    println("***********************************************************************************************")
    println("***********************************************************************************************")

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = $" +WSSSE)

    // Save and load model
    //clusters.save(sc, "KMeansModel")
    //val sameModel = KMeansModel.load(sc, "KMeansModel")
    // $example off$

    println("***********************************************************************************************")
    sc.stop()
  }
}
=
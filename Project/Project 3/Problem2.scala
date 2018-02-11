package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem2 {
  def main(args: Array[String]) {
    // Init
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)

    // Definitions
    val from = 0
    val to = 1
    
    // Core
    val input = sc.textFile(inputFile)
    val graph = input.map(_.split("\t"))
    
    val reversed = graph.map(x =>( x(to), x(from)))
    
    val processed = reversed.reduceByKey(_ + "\t" + _)
                          .map(x => (x._1.toInt, x._2.split("\t").toList.sortWith(_.toInt < _.toInt).mkString(",")))
    
    val results = processed.sortByKey()
                         .map(x => x._1 + "\t" + x._2)
                         
    // Output
    results.saveAsTextFile(outputFolder)
  }
}
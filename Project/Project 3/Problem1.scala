package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem1 {
  def main(args: Array[String]) {
    // Init
    val inputFile = args(0)
    val outputFolder = args(1)
    val k = Integer.parseInt(args(2))
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)

    // Core
    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.toLowerCase().split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").toSet)
                     .filter(x => x.length() > 0 && x.charAt(0) >= 'a' && x.charAt(0) <= 'z')
    
    val sorted = words.map(word => (word, 1))
                      .reduceByKey(_ + _)
                      .sortByKey()
                      .map(_.swap)
                      .sortByKey(false)
                      .map(x => x._2 + "\t" + x._1)
    
    val topK = sorted.take(k)
    
    // Output
    sc.parallelize(topK).saveAsTextFile(outputFolder)
  }
}
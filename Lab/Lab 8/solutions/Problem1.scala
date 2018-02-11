package comp9313.lab8

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("adf")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)    
    val words = textFile.flatMap(_.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+"))
    
    val counts = words.filter(x => x.length >= 1).map(x => x.toLowerCase).
      filter(x => x.charAt(0) <='z' && x.charAt(0) >='a').map(x => (x.charAt(0), (1, x.length())))
    
    val aggr = counts.reduceByKey( (a, b) => (a._1+b._1, a._2+b._2)).sortByKey()
    aggr.foreach(x => println(x._1, x._2._2.toDouble/x._2._1))
    //aggr.map(x => (x._1, x._2._2.toFloat/x._2._1)).saveAsTextFile(outputFolder)
    aggr.map(x => s"${x._1},${x._2._2.toDouble/x._2._1}").saveAsTextFile(outputFolder)
  }
}

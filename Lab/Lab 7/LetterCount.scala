package comp9313.lab7

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object LetterCount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("letter count").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val words = textFile.flatMap(_.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+"))

    val counts = words.filter(x => x.length >= 1).map(x => x.toLowerCase).
      filter(x => x.charAt(0) <= 'z' && x.charAt(0) >= 'a').map(x => (x.charAt(0), 1)).reduceByKey(_+_).sortByKey()

      counts.foreach(println)
    //counts.saveAsTextFile(outputFolder)
  }
}
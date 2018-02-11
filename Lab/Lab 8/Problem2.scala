package comp9313.lab8

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Problem2 {
  var Id = 0
  var PostId = 1
  var VoteTypeId = 2
  var UserId = 3
  var CreationTime = 4

  def Question1(textFile: RDD[Array[String]]) {
    //add your code here
  }

  def Question2(textFile: RDD[Array[String]]) {
    //add your code here
  }

  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("lab8").setMaster("local")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(inputFile).map(_.split(","))    
    Question1(textFile)
    Question2(textFile)
  }
}

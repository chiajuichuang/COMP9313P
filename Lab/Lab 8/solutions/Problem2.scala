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

  def Question2(textFile: RDD[Array[String]]) {
       
    val puPair = textFile.filter(_(VoteTypeId) == "5").map(x => (x(PostId), x(UserId))).distinct
    
    val res = puPair.groupByKey().filter(_._2.size>10).mapValues(x => x.toList.sortBy(_.toInt)).sortBy(_._1.toInt)    
    val fmtres = res.map{case (x, y) => s"""$x#${y.mkString(",")}"""}
    fmtres.foreach(println)
  }
  
  def Question1(textFile: RDD[Array[String]]) {
    
    val pvPair = textFile.map(x => (x(VoteTypeId), x(PostId))).distinct
    //val res = pvPair.countByKey().toSeq.sortWith(_._2 > _._2).take(5).map{case (x, y)=> s"$x\t$y"}
    val res = pvPair.countByKey().toSeq.sortWith(_._2 > _._2).take(5)
    
    res.foreach(x => println(x._1+"\t"+x._2))
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

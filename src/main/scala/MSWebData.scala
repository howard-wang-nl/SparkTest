import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{InputSplit, JobConf, RecordReader, Reporter, TextInputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** Data file sample lines
  * I,4,"www.microsoft.com","created by getlog.pl"
  * T,1,"VRoot",0,0,"VRoot"
  * N,0,"0"
  * N,1,"1"
  * T,2,"Hide1",0,0,"Hide"
  * N,0,"0"
  * N,1,"1"
  * A,1277,1,"NetShow for PowerPoint","/stream"
  * A,1253,1,"MS Word Development","/worddev"
  * A,1109,1,"TechNet (World Wide Web Edition)","/technet"
  * C,"10001",10001
  * V,1038,1
  * V,1026,1
  * V,1034,1
  * C,"10002",10002
  * V,1008,1
  * V,1056,1
  */

/** Data model
  */
case class MSWebDataRecLine(id: Char, rest: List[String])
case class MSWebDataRecI(n: Int, str: String, comment: String)
case class MSWebDataRecT(n: Int, str: String, i1: Int, i2: Int, str2: String)
case class MSWebDataRecN(n: Int, str: String)
case class MSWebDataRecA(id: Int, n: Int, title: String, url: String)
case class MSWebDataRecC(str: String, id: Int)
case class MSWebDataRecV(id: Int, n: Int)
case class MSWebDataRecCV(cid: Int, v: List[MSWebDataRecV])

object MSWebData {
  val inputFile = "/Users/hwang/IdeaProjects/SparkTest/data/anonymous-msweb.test"
  val outputFile = "/Users/hwang/IdeaProjects/SparkTest/data/anonymous-msweb.rdd"

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("MSWebData")
    val sc = new SparkContext(conf)
 //   val s = sc.textFile(inputFile)
    val rawData: RDD[(LongWritable, Text)] = sc.hadoopFile[LongWritable, Text, MSWDInputFormat](inputFile)
//    rawData.foreach(x => println("%d, %s".format(x._1.get, x._2.toString)))
    rawData.saveAsTextFile(outputFile)
  }
}

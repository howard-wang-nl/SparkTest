import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val inputPath = "/Users/hwang/IdeaProjects/githubstat/data/"

    val files = (new java.io.File(inputPath)).listFiles
    val filesSel = files.filter(f=>f.getName.endsWith(".json"))
    val textFile = filesSel.map(f =>sc.textFile(f.getPath))
    val eventType = textFile.map(_.filter(line => line.contains("PushEvent")))
    println(eventType.map(_.count()).reduce(_ + _))
  }
}


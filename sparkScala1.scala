import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object sparkScala1 {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val logFile = "D:\\__Riz__\\untitled\\src\\main\\scala\\README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

}

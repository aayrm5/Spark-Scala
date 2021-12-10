import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object customer_orders extends App{
  
  Logger.getLogger("org").setLevel(Level.INFO)
  
  val sc = new SparkContext("local[*]","customer_orders")
  
  val input = sc.textFile("D:/__Riz__/TrendyTech/week9 - Spark1/customerorders-201008-180523.csv")
  
  val transf = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  .reduceByKey((x,y) => x+y)
  .filter(x => x._2 > 5000)
  .map(x => (x._1, x._2*2))
  .sortBy(x => x._2,false)
  
  val result = transf.collect.foreach(println)
  println(transf.count)

  //To save the results in a disk/HDFS, pass the output folder path, *not the file name*.
//  transf.saveAsTextFile("D:/__Riz__/TrendyTech/week9 - Spark1/transf_customerorders") 
  
//  scala.io.StdIn.readLine()
  
}
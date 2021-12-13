import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object movie_data extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","movie-data")
  
  val input = sc.textFile("D:/__Riz__/TrendyTech/week9 - Spark1/moviedata-201008-180523.data")
  
  val transf = input.map(x => x.split("\t")(2)) // extract only the third column from the input
  .map(x => (x,1))                // convert the single column values into tuple pair
  .reduceByKey((x,y) => x+y)      // add the values based on key
  .sortBy(x => x._2,false)        // Sort by values, false - descending first
  
  transf.collect.foreach(println)

//  val res = transf.countByValue  
  
    /*it is an action and returns a Map Structure. 
    Cannot perform any RDD operations on a Map(no sortBy's). 
    Also, it is stored on the local machine, thus parallelism is lost.*/
  
//  res.foreach(println)

  

}
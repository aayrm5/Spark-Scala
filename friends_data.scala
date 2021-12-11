import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object friends_data extends App{
  
  //writing a namedFunc to be used inside, as a parameter to map()
  def parseLine(line:String) = {
    val fields =line.split("::")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","friends_data")
  
  // we want to find average no. of connections for each age
  //InputFileSchema - row_id, name, age, no_of_connections

  val input = sc.textFile("D:/__Riz__/TrendyTech/week9 - Spark1/friendsdata-201008-180523.csv")
  
  val transf = input.map(parseLine) // creates (33, 100) - a tuple of age & num of friends
  
//  val lenMap =transf.map(x => (x._1,(x._2,1))) // creates (33, (100,1)) - using mapValues is easier when transforming values
  
  val lenMapValues = transf.mapValues(x => (x,1)) // creates (33, (100,1))
  
  val totalsByAge = lenMapValues.reduceByKey((x,y) => (x._1+y._1 , x._2+y._2))   //   we want ans as (33, (600, 3))
  
//  val averagesByAge = totalsByAge.map(x => (x._1, x._2._1/x._2._2))  // x._2 = (600,3)
  
  val averagesByAge = totalsByAge.mapValues(x => x._1/x._2) //x = (600,3)
  
  //averagesByAge would be (age, avg_no_friend) => (33, 200)
  
  val sorted_avg = averagesByAge.sortBy(x => x._2,false)
  
  sorted_avg.collect.foreach(println)
  
  
}
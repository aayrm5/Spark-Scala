import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object RatingsCalculator extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "ratingsCalculator")
  
  val ratingsRDD = sc.textFile("D:/__Riz__/TrendyTech/week9 - Spark1/ratings-201014-183159.dat")
  
  val mappedRDD = ratingsRDD.map(x => {
    val fields = x.split("::")
    (fields(1), fields(2))
  })
  //INPUT
  //(1193,5)
  //(1193,3)
  //(1193,4)
  
  //OUTPUT
  // (1195, (5.0,1.0))
  // (1195, (3.0,1.0))
  // (1195, (4.0,1.0))

//  val furtherMapped = mappedRDD.map(x => ( x._1, (x._2.toFloat,1.0) ) ) 
  // Using mapValues instead of map as we are only operating on values.
  
  val furtherMapped = mappedRDD.mapValues (x => (x.toFloat,1.0) )
  
  val reducedRDD = furtherMapped.reduceByKey( (x,y) => (x._1+y._1 , x._2+y._2) ) // (1195, (12.0,3.0))
  
  val filteredRDD = reducedRDD.filter(x => x._2._2 > 10) //number of ratings -gt 10
  
  val filter2RDD = filteredRDD.mapValues(x => x._1/x._2) // (1195, 4.0) (movie_id, ratings)
  
  val ratingsProcessed = filter2RDD.filter(x => x._2 > 4.0) //ratings greater than 4.0
  
//  ratingsProcessed.collect().foreach(println)
  
  // Loading the movies.dat file to map (JOIN) the movie names to the movie id.
  
  val moviesRDD = sc.textFile("D:/__Riz__/TrendyTech/week9 - Spark1/movies-201014-183159.dat")
  
  val moviesMapped = moviesRDD.map(x => {
    val fields = x.split("::")
    (fields(0), fields(1))
  })

  //before joining
  // (101, Toy Story)
  // (101, 4.0)

  //after joining
  //(101, (4.0, Toy Story))
  
  val joinedRDD = moviesMapped.join(ratingsProcessed)
  
  val finalRDD = joinedRDD.map(x => (x._2._1, x._2._2)).sortBy(x => x._2, false) 
  //grabbing the movie name and the ratings and sorting them descending.
  
  finalRDD.collect().foreach(println)
  
}
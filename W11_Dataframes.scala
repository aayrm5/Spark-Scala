import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger


object W11_Dataframes extends App{
  
//  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DataFrames Session")
  sparkConf.set("spark.master", "local[*]")
  
  val spark = SparkSession.builder()
//  .appName("DataFrames Session")
//  .master("local[*]") //these properties are included in sparkConf.
  .config(sparkConf) //instead of the above properties, sparkConf is mentioned in the config()
  .getOrCreate()
  
  // processing happens here
  val ordersDF = spark.read
  .option("header",true) 
//  .option("inferSchema", true) //costly operation - reads the data to infer the schema so performs an additional job. In that additional job, it deserializes the data to object to infer the schema.
  .csv("D:/__Riz__/TrendyTech/week9 - Spark1/orders-201019-002101.csv")
  
  val groupedOrdersDF = ordersDF.repartition(4)
  .where("order_customer_id > 10000")
  .select("order_id", "order_customer_id")
  .groupBy("order_customer_id")
  .count() //It's a transformation, not an action.
  
  
//  groupedOrdersDF.foreach(x => println(x) ) //Low-Level code which would be sent to the executor without any further compilation.
    
  groupedOrdersDF.show()
//  ordersDF.printSchema()
  
  //Logging for debugging
  Logger.getLogger(getClass.getName).info("My Application is ran successfully")
  
  scala.io.StdIn.readLine()
  spark.stop()
}
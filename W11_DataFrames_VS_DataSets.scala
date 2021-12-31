


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

import java.sql.Timestamp

case class OrdersData(order_id: Int, order_date: Timestamp, order_customer_id:Int, order_status:String)
//val encoder = org.apache.spark.sql.Encoders.product[OrdersData]

object W11_DataFrames_VS_DataSets extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","DataFrames VS DataSets")
  sparkConf.set("spark.master","local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDF: Dataset[Row]= spark.read  //DataFrame is a DataSet of type Row
    .option("header", true)
    .option("inferSchema",true)
    .csv("D:/__Riz__/TrendyTech/week9 - Spark1/orders-201019-002101.csv")

  ordersDF.filter("order_id < 10") //valid cond" as order_id column exists.

  //below ex is invalid cond" as order_ids column doesn't exists and there is no compile time exception provided for DataFrames.
  //  ordersDF.filter("order_ids < 10") //throws an error if ran, as "order_ids" column is not there.

  //By defining case class, we will convert the DataFrame to DataSet.
  import spark.implicits._
  val ordersDS = ordersDF.as[OrdersData] //OrdersData is a case class defined at the start


  //using a anonymous function with DataSet
  ordersDS.filter(x => x.order_id < 10).show() //valid statement as order_id is an available column

  //  ordersDS.filter(x => x.order_ids < 10) //throws an error at compile time itself as ordersDS is a dataset and is defined using a case class.



  scala.io.StdIn.readLine()
  spark.stop()

}
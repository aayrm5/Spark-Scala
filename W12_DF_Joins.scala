import W12_DF_Joins.joinedDF
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, desc}

object W12_DF_Joins extends App {

//  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()

  sparkConf.set("spark.app.name", "DF Joins")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder
    .config(conf = sparkConf)
    .getOrCreate()

  val ordersDF = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "D:/__Riz__/TrendyTech/week9 - Spark1/orders-201025-223502.csv")
    .load()

//  val ordersNew = ordersDF.withColumnRenamed("customer_id", "cust_id")

  ordersDF.printSchema()

  val customersDF = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "D:/__Riz__/TrendyTech/week9 - Spark1/customers-201025-223502.csv")
    .load()

  customersDF.printSchema()
// Joining the dataframes on the commom id.
  val joinCondition = ordersDF.col("customer_id") === customersDF.col("customer_id")

  val joinedDF = ordersDF.join(customersDF, joinCondition, "outer")
    .drop(ordersDF.col("customer_id"))
    .select("order_id","customer_id","customer_fname")
    .sort(desc("order_id"))
    .withColumn("order_id", expr("coalesce(order_id, -1)"))

  joinedDF.printSchema()

  joinedDF.show(50,false)

  scala.io.StdIn.readLine()

  spark.stop()
}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object W12_Aggregations extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Aggregations")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  /*val inputDF = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "D:/__Riz__/TrendyTech/week9 - Spark1/order_data-201025-223502.csv")
    .load()*/


  //Simple Aggregations

  /* //Column Object Notation - Simple Aggregation
   inputDF.select(
     count("*").as("RowCount"),
     sum("Quantity").as("TotalQuantity"),
     avg("UnitPrice").as("AvgPrice"),
     countDistinct("InvoiceNo").as("CountDistinct")
   ).show()

   //String Notation - Simple Aggregation
   inputDF.selectExpr(
       "count(StockCode) as RowCount",
       "sum(Quantity) as TotalQuantity",
       "avg(UnitPrice) as AvgPrice",
       "count(Distinct(InvoiceNo)) as CountDistinct"
       ).show()

   // The SQL way of doing things.

   inputDF.createOrReplaceTempView("sales")

   spark.sql("select count(*), sum(Quantity), avg(UnitPrice), count(distinct(InvoiceNo)) from sales").show()

 //  inputDF.show()
 */

  //Grouped Aggregations

  //Column-Object Expression
/*  val summaryDF = inputDF.groupBy("Country", "InvoiceNo")
    .agg(sum("Quantity").as("TotalQuantity"),
      sum(expr("Quantity * UnitPrice")).as("InvoiceValue"))

  //String Expressions
  val summaryDF1 = inputDF.groupBy("Country","InvoiceNo")
    .agg(expr("sum(Quantity) as TotalQuantity"),
          expr("sum(Quantity * UnitPrice) as InvoiceValue"))

  //Spark SQL way
  inputDF.createOrReplaceTempView("sales")

  val summaryDF2 = spark.sql(
    """select Country, InvoiceNo, sum(Quantity) as TotalQuantity, sum(Quantity * UnitPrice) as InvoiceValue
      from sales group by Country, InvoiceNo""")

  summaryDF2.show()*/

  //Window Aggregates

  val windowDF = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "D:/__Riz__/TrendyTech/week9 - Spark1/windowdata-201025-223502.csv")
    .load().toDF("country", "weeknum", "quantity", "amount", "invoicevalue")

//  windowDF.printSchema()

  //Defining the Window Spec
  val myWindow = Window.partitionBy("country")
    .orderBy("weeknum")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val mydf = windowDF.withColumn("RunningTotal", sum("invoicevalue")
    .over(myWindow))

  mydf.show()



  spark.stop()

}
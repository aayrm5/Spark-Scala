import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object W12_Process_LargeTable extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Join On Large Table")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder
    .config(conf = sparkConf)
    .getOrCreate()

  val inputDF = spark.read
    .option("header", true)
    .csv("D:/__Riz__/TrendyTech/week9 - Spark1/biglogW12.txt")

//  inputDF.show()

  inputDF.createOrReplaceTempView("LoggingTable")

  val results = spark.sql(
    """select level, date_format(datetime, 'MMMM') as month,
      |count(1) as total from LoggingTable group by level, month""".stripMargin)

  results.createOrReplaceTempView("ResultsTable")

  val results_new = spark.sql(
    """select level, date_format(datetime, 'MMMM') as month, cast(first(date_format(datetime, 'M')) as int) as month_num,
      |count(1) as total from LoggingTable group by level, month order by month_num, level""".stripMargin)
    .drop("month_num")
    .show(false)

  val columns = List("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")

  val results_refined = spark.sql(
    """select level, date_format(datetime, 'MMMM') as month
      |from LoggingTable""".stripMargin)
    .groupBy("level")
    .pivot("month", columns).count()
    .show()


  spark.stop()
}

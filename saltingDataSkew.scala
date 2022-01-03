import org.apache.spark.SparkContext

object saltingDataSkew extends App{

  val sc = new SparkContext("local[*]", "SaltingDataSkew")

  val random = new scala.util.Random
  val start = 1
  val end = 60

  val rdd1 = sc.textFile("D:\\__Riz__\\TrendyTech\\week 9 - Spark1\\bigLogNew.txt")

  val rdd2 = rdd1.map(x => {
    var num = start + random.nextInt( (end-start) + 1 )
    (x.split(":")(0) + num, x.split(":")(1))
  })

//  val rdd2 = rdd1.map(x => {
//    val fields = x.split(":")
//    (fields(0), fields(1))
//  })
  val rdd3 = rdd2.groupByKey

  val rdd4 = rdd3.map(x => (x._1, x._2.size))

  val rdd5 = rdd4.map(x => {
    if(x._1.substring(0,4) == "WARN")
      ("WARN", x._2)
      else ("ERROR", x._2)
  })

  val rdd6 = rdd5.reduceByKey(_+_)

  rdd6.collect().foreach(println)

  scala.io.StdIn.readLine()
}

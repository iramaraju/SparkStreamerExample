object weld {

  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.sql.functions._
  import breeze.signal._

  import org.apache.spark.SparkConf
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.streaming.{Seconds, StreamingContext}

  def main( args :Array[String])= {
    val sparkConf = new SparkConf()
      .setSparkHome("/home/ram/dev/spark-1.6.1-bin-hadoop2.6")
      .setMaster("spark://ram-Gazelle:7077")
      .setAppName("weld")
      .set("spark.executor.memory", "1g")
      .set("spark.executor.instances", "8")
      .setJars(Seq("target/scala-2.10/weld_2.10-1.0.jar","target/scala-2.10/spark-csv_2.10-1.2.0.jar","target/scala-2.10/commons-csv-1.2.jar","target/scala-2.10/breeze_2.10-0.11.2.jar"))

    val sc = new org.apache.spark.SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val ipAddress = "192.168.0.116"
    val ipPort = "1123"

    
    val lines = ssc.socketTextStream(ipAddress, ipPort.toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()


  }

  def staticData = {
    val conf = new SparkConf()
      .setSparkHome("/home/ram/dev/spark-1.6.1-bin-hadoop2.6")
      .setMaster("spark://ram-Gazelle:7077")
      .setAppName("weld")
      .set("spark.executor.memory", "1g")
      .set("spark.executor.instances", "8")
      .setJars(Seq("target/scala-2.10/weld_2.10-1.0.jar","target/scala-2.10/spark-csv_2.10-1.2.0.jar","target/scala-2.10/commons-csv-1.2.jar","target/scala-2.10/breeze_2.10-0.11.2.jar"))

    val sc = new org.apache.spark.SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    println("Spark Context Created")
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/home/ram/Downloads/V25.6-WFS285-Low1.csv")
    println("Data Frame Created"+df.count())
    val toInt    = udf[Int, String]( _.toInt)
    val toDouble = udf[Double, String]( _.toDouble)

    val df2 = df.withColumn("Time", toDouble(df("Time(s)"))).withColumn("V", toDouble(df("V"))).withColumn("I", toDouble(df("I"))).withColumn("Sound", toDouble(df("Sound"))).select("Time","V","I","Sound")
    df2.registerTempTable("welddata")
    println("Registered Table"+df2.count())

    sqlContext.sql("Select * From  welddata").take(10).foreach(println)

    sc.stop()
  }

  def server = {
    // Simple server
    import java.net._
    import java.io._
    import scala.io._

    val server = new ServerSocket(9999)
    while (true) {
      val s = server.accept()
      val in = new BufferedSource(s.getInputStream()).getLines()
      val out = new PrintStream(s.getOutputStream())

      out.println(in.next())
      out.flush()
      s.close()
    }

  }
} 

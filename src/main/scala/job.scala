package com.knoldus.Weld

import org.apache.spark.streaming.{Milliseconds, Time}

object weld {

  import org.apache.spark.SparkConf
  import org.apache.spark.sql.functions._
  import breeze.signal._
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  import org.apache.spark.sql.SQLContext
  import akka.actor.ActorSystem
  import akka.actor.Props

  case class Record(time: Double , V: Double,  I :Double, sound : Double)
  def main(args: Array[String]) = {


    val system = ActorSystem("Streamers")
    val streamActor = system.actorOf(Props[Stream],"streamingactor")
    streamActor ! "start"
    Loglevel.setStreamingLogLevels()

    val sparkConf = new SparkConf()
      .setSparkHome("/home/ram/dev/spark-1.6.1-bin-hadoop2.6")
      .setMaster("spark://ram-Gazelle:7077")
      .setAppName("weld")
      .set("spark.executor.memory", "1g")
      .set("spark.executor.instances", "8")
      .setJars(Seq("target/scala-2.10/weld_2.10-1.0.jar", "target/scala-2.10/breeze_2.10-0.11.2.jar"))


    val ssc = new StreamingContext(sparkConf, Milliseconds(5000))


    val ipAddress = "127.0.0.1"
    val ipPort = "1123"
    println("    =========>  IP:" + ipAddress + "Port:" + ipPort)

    val lines = ssc.socketTextStream(ipAddress, ipPort.toInt, StorageLevel.MEMORY_AND_DISK_SER)

    lines.foreachRDD {

      rdd => {
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._

        // Convert RDD[String] to DataFrame

        val schemaString =  "time V I Sound Batch"
        import org.apache.spark.sql.Row;

        // Import Spark SQL data types
        import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType};

        // Generate the schema based on the string of schema
        val schema =
          StructType(
            schemaString.split(" ").map(fieldName => StructField(fieldName, DoubleType, true)))

        // Convert records of the RDD (people) to Rows.
        val rowRDD = rdd.map(_.split(",")).map(p => Row(p(0).toDouble, p(1).toDouble,p(2).toDouble,p(3).toDouble,1.0.toDouble))

        // Apply the schema to the RDD.
        val dataframe = sqlContext.createDataFrame(rowRDD, schema)


        // Register as table
        dataframe.registerTempTable("data")

        // Do word count on DataFrame using SQL and print it
        val outDF =
          sqlContext.sql("select count(*) as Total, avg(V) as Voltage, avg(I) as Current, avg(Sound) as Sound from data group by Batch  ")

        outDF.show()

      }
    }


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
      .setJars(Seq("target/scala-2.10/weld_2.10-1.0.jar", "target/scala-2.10/spark-csv_2.10-1.2.0.jar", "target/scala-2.10/commons-csv-1.2.jar", "target/scala-2.10/breeze_2.10-0.11.2.jar"))

    val sc = new org.apache.spark.SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    println("Spark Context Created")
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/home/ram/Downloads/V25.6-WFS285-Low1.csv")
    println("Data Frame Created" + df.count())
    val toInt = udf[Int, String](_.toInt)
    val toDouble = udf[Double, String](_.toDouble)

    val df2 = df.withColumn("Time", toDouble(df("Time(s)"))).withColumn("V", toDouble(df("V"))).withColumn("I", toDouble(df("I"))).withColumn("Sound", toDouble(df("Sound"))).select("Time", "V", "I", "Sound")
    df2.registerTempTable("welddata")
    println("Registered Table" + df2.count())

    sqlContext.sql("Select * From  welddata").take(10).foreach(println)

    sc.stop()

  }


}


/**
  *
  *   def server = {
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

  def client = {
    import java.net._
    import java.io._
    import scala.io._
    println(InetAddress.getByName("localhost"))
    val s = new Socket("127.0.0.1", 1123)
    val in = s.getInputStream()
    val out = new PrintStream(s.getOutputStream())
    out.println("First Test Record ========================================================")
    Source.fromFile("/home/ram/Downloads/V25.6-WFS285-Low1.csv").getLines().foreach(s=> { out.println(s); Thread.sleep(10) })
    in.close()
    out.flush()
    s.close()
  }


  *
  */


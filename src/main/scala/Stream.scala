package com.knoldus.Weld

import org.apache.commons.io.FileUtils
import akka.actor.Actor


class Stream extends Actor {

def server = {

    // Simple server
    import java.net._
    import java.io._
    import scala.io._

    val server = new ServerSocket(1234,1, InetAddress.getByName("localhost"))

    while (true) {
      val s = server.accept()
      val out = new PrintStream(s.getOutputStream())

      val filename = "/home/ram/Downloads/V25.6-WFS285-Low1.csv"
      Source.fromFile(filename).getLines.drop(1).foreach { line =>
        out.println(line)
          Thread.sleep(1)
      }

      out.flush()
      s.close()
    }
}

  def receive = {
    case "start" => {
      println ( "Starting to  Stream Data")
      server
    }
    case _ => println("Incorrect Message. Streaming unsuccessful !!!!!!!!!!!!")
  }

def client = {
  import java.net._
  import java.io._
  import scala.io._
//  import org.apache.commons.io.FileUtils._

  println(InetAddress.getByName("localhost"))
  val s = new Socket("127.0.1.1", 1234)
  val in = s.getInputStream()
  val out = new PrintStream(s.getOutputStream())
  out.println("First Test Record ========================================================")
  var header = true

  val filename = "/home/ram/Downloads/V25.6-WFS285-Low1.csv"
  for (line <- Source.fromFile(filename).getLines.drop(1)) {
    out.println(line)
    println(line)
    Thread.sleep(1)
  }

  in.close()
  out.flush()
  s.close()
}

}

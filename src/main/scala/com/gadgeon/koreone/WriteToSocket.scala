package com.gadgeon.koreone

import java.io.PrintStream
import java.net.{InetAddress, ServerSocket, Socket}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.BufferedSource

object WriteToSocket {

  def main(args: Array[String]): Unit = {

//    val s = new Socket(InetAddress.getByName("192.168.65.135"), 11008)
//    lazy val in = new BufferedSource(s.getInputStream()).getLines()
//    val out = new PrintStream(s.getOutputStream())
//
//    out.println("Hello, world")
//    out.flush()
//    println("Received: " + in.next())
//
//    s.close()

    val SOCKET_HOST="192.168.65.135"
    val port = 11008
    val s = new Socket(InetAddress.getByName(SOCKET_HOST), port)
    lazy val in = new BufferedSource(s.getInputStream()).getLines()
    val out = new PrintStream(s.getOutputStream())
    println("Speed limit exceeded -> TCP")
    out.println("Speed limit exceeded")
    out.flush()
    in.next()
    s.close()

  }

}

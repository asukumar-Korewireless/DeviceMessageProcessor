package com.gadgeon.koreone

import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.SparkConf

object KafkaStreamToCassandra {
  def setupLogging() = {
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  def parseLine(line: String) = {
    val dateFormat = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

    val fields = line.split(",")
    val id = UUIDs.timeBased()
    val imei = fields(0)
    val actualDate = dateFormat.parse(fields(1))
    val latitude = fields(2).toFloat
    val longitude = fields(3).toFloat
    val direction = fields(4).toFloat
    val odometer = fields(5)
    val speed = fields(6)
    //val analog = fields(7)
    val temperature = fields(8)
    //val eventCode = fields(9)
    //val textM = fields(10)
    val fuel = fields(11)
    //val temp2 = fields(12)
    val voltage = fields(13)
    (id, imei, actualDate, latitude, longitude,
      direction, odometer, speed, temperature, fuel, voltage)
  }

  def main(args: Array[String]) = {
    val cassandraHost = args(0) // localhost
    val kafkaServers = args(1) // localhost:9092

    // Create the context with a 1 second batch size
    //val ssc = new StreamingContext("local[*]", "Kafka Streaming", Seconds(1))
    val conf = new SparkConf().setAppName("KafkaStreamToCassandra")
      .set("spark.cassandra.connection.host", cassandraHost)
    val ssc = new StreamingContext(conf, Seconds(1))

    setupLogging()

    // hostname:port of Kafka brokers, not Zookeeper
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // List of topics you want to listen for from Kafka
    val topics = Array("devicemessages")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(_.value)

    val parsedLine = lines.map(parseLine)

    val columns = SomeColumns("id", "imei", "actual_date", "latitude", "longitude",
      "direction", "odometer", "speed", "temperature", "fuel", "voltage")

    parsedLine.saveToCassandra("koreone", "device_messages", columns)

    ssc.checkpoint("c:\\tmp")
    ssc.start()
    ssc.awaitTermination()
  }



}

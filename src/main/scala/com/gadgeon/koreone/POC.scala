package com.gadgeon.koreone

import java.util.{Properties, TimeZone}

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Json

object POC {
  def setupLogging() = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("com").setLevel(Level.ERROR)
  }

  def parseLine(line: String) = {
//    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
//    //println(line)
//    val cleanedLine = line.replace("\"", "")
//    val fields = cleanedLine.split(",")
//    val id = UUIDs.timeBased()
//    val imei = fields(0)
//    val actualDate = dateFormat.parse(fields(1))
//    val latitude = fields(2).toFloat
//    val longitude = fields(3).toFloat
//    val direction = fields(4).toFloat
//    val odometer = fields(5)
//    val speed = fields(6).toFloat
//    //val analog = fields(7)
//    val temperature = fields(8)
//    //val eventCode = fields(9)
//    //val textM = fields(10)
//    val fuel = fields(11)
//    //val temp2 = fields(12)
//    val voltage = fields(13)
//    //println(actualDate)
//    (id, imei, actualDate, latitude, longitude,
//      direction, odometer, speed, temperature, fuel, voltage)
    (line)

  }

  def main(args: Array[String]) = {
    // Configuration Variables
//    val CASSANDRA_HOST = args(0) //"localhost"
//    val KAFKA_SERVERS = args(1) //"localhost:9092"
//    val INPUT_TOPIC = args(2) //"aaa-devicemessages"
//    val OUTPUT_TOPIC = args(3) //"aaa-speedanalysis"

    val CASSANDRA_HOST = "localhost"//"192.168.65.146" //args(0)
    val KAFKA_SERVERS = "localhost:9092" // "192.168.65.175:9092"//"172.30.100.208:9092" //172.30.100.208:9092" //args(1) //
    val INPUT_TOPIC = "test" //args(2)
    val OUTPUT_TOPIC = "aaa-speedanalysis" //args(3)

    val CASSANDRA_KEYSPACE = "koreone"
   // val CASSANDRA_TABLE = "device_messages"
    val CASSANDRA_TABLE = "AlertMessage"
//    val CASSANDRA_COLUMNS = SomeColumns("id", "imei", "actual_date", "latitude", "longitude",
//      "direction", "odometer", "speed", "temperature", "fuel", "voltage")
    val CASSANDRA_COLUMNS = SomeColumns("imei","portno","speed")
    val CHECKPOINT_PATH = "c:\\tmp"


    setupLogging()

    // Create the context with a 1 second batch size
    //val ssc = new StreamingContext("local[*]", "Kafka Streaming", Seconds(1))
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("POC")
      .set("spark.cassandra.connection.host", CASSANDRA_HOST)
    val ssc = new StreamingContext(conf, Seconds(1))

    // hostname:port of Kafka brokers, not Zookeeper
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> KAFKA_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // List of topics you want to listen for from Kafka
    val topics = Array(INPUT_TOPIC)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(_.value)

    //lines.saveToCassandra(CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CASSANDRA_COLUMNS)
     val parsedLine = lines.map(parseLine)

  //  val cachedStream = parsedLine.cache()

    parsedLine.saveToCassandra(CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CASSANDRA_COLUMNS)

//    val props = new Properties()
//    props.put("bootstrap.servers", KAFKA_SERVERS)
//    props.put("client.id", "Producer")
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//
//    val avgFn = (x:(Float, Int), y: (Float, Int)) => (x._1 + y._1, x._2 + y._2)
//    val speedData = cachedStream.map(x => (x._2, x._8)).mapValues(s => (s, 1))
//    val averageSpeed = speedData.reduceByKeyAndWindow(avgFn, Seconds(30), Seconds(5))
//      .mapValues((x) => x._1 / x._2)
//
//    // Writing the speed analysis data(average speed) to Kafka
//    averageSpeed.foreachRDD(rdd => {
//      rdd.foreachPartition(partition => {
//        val producer = new KafkaProducer[String, String](props)
//        if(partition.hasNext) {
//          partition.foreach(avgMsg => {
//            //println(avgMsg)
//            val message = avgMsg._1.concat(",").concat(avgMsg._2.toString)
//            println(message)
//            val record:ProducerRecord[String, String] = new ProducerRecord(OUTPUT_TOPIC, message)
//            producer.send(record)
//          })
//        }
//      })
//    })

    ssc.checkpoint(CHECKPOINT_PATH)
    ssc.start()
    ssc.awaitTermination()
  }



}

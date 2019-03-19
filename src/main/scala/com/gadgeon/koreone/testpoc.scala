package com.gadgeon.koreone
import java.io.PrintStream
import java.net.{InetAddress, Socket}
import java.sql.Time
import java.time.OffsetDateTime
import java.util.{Date, Properties, TimeZone}
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
import org.codehaus.jettison.json.JSONObject
import scala.io.BufferedSource
object testpoc {
  def setupLogging() = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("com").setLevel(Level.ERROR)
  }
  //  def parseLine(line: String) = {
  //
  //    val json: JSONObject = new JSONObject(line);
  //    println(json)
  //    json
  ////    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  ////    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
  ////    //println(line)
  ////    val cleanedLine = line.replace("\"", "")
  ////    val fields = cleanedLine.split(",")
  ////    val id = UUIDs.timeBased()
  ////    val imei = fields(0)
  ////    val actualDate = dateFormat.parse(fields(1))
  ////    val latitude = fields(2).toFloat
  ////    val longitude = fields(3).toFloat
  ////    val direction = fields(4).toFloat
  ////    val odometer = fields(5)
  ////    val speed = fields(6).toFloat
  ////    //val analog = fields(7)
  ////    val temperature = fields(8)
  ////    //val eventCode = fields(9)
  ////    //val textM = fields(10)
  ////    val fuel = fields(11)
  ////    //val temp2 = fields(12)
  ////    val voltage = fields(13)
  ////    //println(actualDate)
  ////    (id, imei, actualDate, latitude, longitude,
  ////      direction, odometer, speed, temperature, fuel, voltage)
  //  }
  def parseLine(line: String) = {
    val json = new JSONObject(line)
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
    val portNumber = json.get("PortNumber")
    val imei = json.get("IMEI")
    val ActualDate = dateFormat.parse(json.get("ActualDate").toString)
    val UnprocessedCreated = dateFormat.parse(json.get("UnprocessedCreated").toString)
    val sensors = json.getJSONArray("Sensors")
    val speedSensors = (0 until sensors.length).map(sensors.getJSONObject)
      .filter(x => x.get("Name")== "Speed")
    var speed = 0f;
    speedSensors.foreach(s => {
      speed = s.getDouble("Value").toFloat
    })
    (imei,portNumber, speed, ActualDate, UnprocessedCreated)
  }
  def writeToTcp(message: String) = {
    val SOCKET_HOST="192.168.65.135"
    val fields = message.split(",")
    val port = 11008 //fields(0).toInt
    val s = new Socket(InetAddress.getByName(SOCKET_HOST), port)
    lazy val in = new BufferedSource(s.getInputStream()).getLines()
    val out = new PrintStream(s.getOutputStream())
    println("Speed limit exceeded -> TCP")
    out.println("Speed limit exceeded")
    out.flush()
    in.next()
    s.close()
  }
  def main(args: Array[String]) = {
    // Configuration Variables
    //    val CASSANDRA_HOST = args(0) //"localhost"
    //    val KAFKA_SERVERS = args(1) //"localhost:9092"
    //    val INPUT_TOPIC = args(2) //"aaa-devicemessages"
    //    val OUTPUT_TOPIC = args(3) //"aaa-speedanalysis"
    val CASSANDRA_HOST = "localhost"//"DIBINLALTP-605" //localhost"//"192.168.65.146" //args(0)
    val KAFKA_SERVERS = "192.168.65.173:9092"//"JESMIPK-615:9092" // "192.168.65.175:9092"//"172.30.100.208:9092" //172.30.100.208:9092" //args(1) //
    val INPUT_TOPIC = "test" //args(2)
    val OUTPUT_TOPIC = "aaa-speedanalysis" //args(3)
    val CASSANDRA_KEYSPACE = "koreone"
    val CASSANDRA_TABLE1 = "speed"
    val CASSANDRA_TABLE2 = "sample" //"Avgspeed"
    val CASSANDRA_COLUMNS1 = SomeColumns("imei","portno","speed", "actualdate","unprocessedcreated")
    val CASSANDRA_COLUMNS2= SomeColumns("imei","avgspeed","windowtime")
    val CHECKPOINT_PATH = "c:\\tmp"
    val SPEED_LIMIT = 150
    setupLogging()
    // Create the context with a 1 second batch size
    //val ssc = new StreamingContext("local[*]", "Kafka Streaming", Seconds(1))
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("KafkaStreamToCassandra")
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
    var window_time = ""
    val lines = stream.map(_.value)
    val parsedLine = lines.map(parseLine)
    val reprocessrdd =  parsedLine.filter(l=> OffsetDateTime.parse(l._4.toString).toEpochSecond < (OffsetDateTime.parse(window_time).toEpochSecond -120))
  //  if (reprocessrdd.)
    //println(parsedLine)
    val cachedStream = parsedLine.cache()
    parsedLine.saveToCassandra(CASSANDRA_KEYSPACE, CASSANDRA_TABLE1, CASSANDRA_COLUMNS1)
    val avgFn = (x:(Float, Int), y: (Float, Int)) => (x._1 + y._1, x._2 + y._2)
    val speedData = cachedStream.map(x => (x._1, x._3)).mapValues(s => (s, 1))
    val averageSpeed = speedData.reduceByKeyAndWindow(avgFn,Seconds(120),Seconds(120))
      .mapValues((x) => x._1 / x._2)
      .map(x => (x._1, x._2))
    averageSpeed.foreachRDD((rdd,time)=> {
      window_time = time.toString
    }
    )
    averageSpeed.map(a=>(a._1,a._2,window_time)).saveToCassandra(CASSANDRA_KEYSPACE, CASSANDRA_TABLE2, CASSANDRA_COLUMNS2)
    // Writing the speed analysis data(average speed) to Kafka
    //        averageSpeed.foreachRDD(rdd => {
    //          rdd.foreachPartition(partition => {
    //            val producer = new KafkaProducer[String, String](props)
    //            if(partition.hasNext) {
    //              partition.foreach(avgMsg => {
    //                println(avgMsg)
    //                val message = avgMsg._1.concat(",").concat(avgMsg._2.toString)
    //                println(message)
    //                val record:ProducerRecord[String, String] = new ProducerRecord(OUTPUT_TOPIC, message)
    //                producer.send(record)
    //              })
    //            }
    //          })
    //        })
    // Writing the speed analysis data(average speed) to Kafka
    val overSpeedData = parsedLine.filter(s => s._3 >= SPEED_LIMIT)
    val props = new Properties()
    props.put("bootstrap.servers", KAFKA_SERVERS)
    props.put("client.id", "Producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    overSpeedData.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](props)
        if(partition.hasNext) {
          partition.foreach(overSpeedItem => {
            val message = overSpeedItem._1.toString().concat(",")
              .concat(overSpeedItem._2.toString).concat(",")
              .concat(overSpeedItem._3.toString)
            println(message)
            val record:ProducerRecord[String, String] = new ProducerRecord(OUTPUT_TOPIC, message)
            producer.send(record)
            // Write the overspeed data to TCP port
            writeToTcp(message)
          })
        }
      })
    })
    ssc.checkpoint(CHECKPOINT_PATH)
    ssc.start()
    ssc.awaitTermination()
  }
}
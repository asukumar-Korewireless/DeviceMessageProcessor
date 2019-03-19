package com.gadgeon.koreone
import java.io.PrintStream
import java.net.{InetAddress, Socket}
import java.time.OffsetDateTime
import java.util.{Calendar, Date, Properties, TimeZone}

import com.datastax.driver.core.utils.UUIDs
//import org.apache.spark.sql.cassandra.CassandraSQLContext
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
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource
object PortwiseProcess {

  var WindowList = new ListBuffer[Long]()
  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
 // dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))

  def setupLogging() = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)
  }
  def parseLine(line: String) = {
    val json = new JSONObject(line)
    //val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
   // dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
    val portNumber = json.get("PortNumber")
    val imei = json.get("IMEI")
   val ActualDate = json.get("ActualDate").toString //dateFormat.format(Calendar.getInstance().getTime())
     // .toString//dateFormat.parse(json.get("ActualDate").toString) Calendar.getInstance()//dateFormat.parse(json.get("ActualDate").toString)
     val UnprocessedCreated = json.get("UnprocessedCreated").toString
    //val UnprocessedCreated = dateFormat.format(Calendar.getInstance().getTime()).toString//json.get("UnprocessedCreated").toString
   // val UnprocessedCreated = "2019-03-15T17:43:10"
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
    val KAFKA_SERVERS =  "localhost:9092"//"localhost:9092"//"JESMIPK-615:9092" // "192.168.65.175:9092"//"172.30.100.208:9092" //172.30.100.208:9092" //args(1) //
    val INPUT_TOPIC = "test" //args(2)
    val OUTPUT_TOPIC = "aaa-speedanalysis" //args(3)
    val CASSANDRA_KEYSPACE = "koreone"
    val CASSANDRA_TABLE1 = "speed"
    val CASSANDRA_TABLE2 = "sample" //"Avgspeed"
    val CASSANDRA_COLUMNS1 = SomeColumns("id","imei","portno","speed", "actualdate","unprocessedcreateddate")
    val CASSANDRA_COLUMNS2= SomeColumns("id","imei","avgspeed","windowendtime")
    val CHECKPOINT_PATH = "c:\\tmp"
    val SPEED_LIMIT = 15000
    val window_duration = 120;
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
    val props = new Properties()
    props.put("bootstrap.servers", KAFKA_SERVERS)
    props.put("client.id", "Producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // List of topics you want to listen for from Kafka
    val topics = Array(INPUT_TOPIC)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val lines = stream.map(_.value)
    val parsedLine = lines.map(parseLine)
    val cachedStream = parsedLine.cache()
    //  var e = System.currentTimeMillis().toString
    var first :Long = 0 ;
    var last : Long = 0;
    var window_time = ((System.currentTimeMillis()/1000) + window_duration).toString//"0"//OffsetDateTime.parse(Calendar.getInstance().toString).toEpochSecond.toString
   // WindowList +=((System.currentTimeMillis()/1000) + window_duration)
    var WindowData = new ListBuffer[String]()
    val reprocessrdd =  parsedLine.filter(l=> {
     // val dateFormat1 = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
     // var dateFormat1uptime = dateFormat1.format(l._5)
      println((dateFormat.parse(l._5).getTime/1000).toString + "," + (window_time.toLong + window_duration).toString )
      dateFormat.parse(l._5).getTime/1000 < window_time.toLong + window_duration
    })
    reprocessrdd.foreachRDD(z=>z.foreach(k=>println("REPROCESSED RDD :" + k)))
   // reprocessrdd.foreachRDD(x=>x.foreach(f=>println(OffsetDateTime.parse(f._5.toString).toEpochSecond.toString+","+window_time.toString)));
    val reprocessrdd2 = reprocessrdd.map(a=>(a._1, dateFormat.parse(a._5).getTime/1000 + window_duration ,a._3 ))
    reprocessrdd2.foreachRDD(rdd=>rdd.foreachPartition(a=>{
      a.foreach(b=>{
       println("REPROCESSED RDD 2 :" +b)
        last = b._2
        first = b._2 - window_duration
        WindowList.foreach(k=>println("WINDOW LIST :" + k))
        WindowList.filter(x=>(x> first && x < last)).foreach(z=>
        { println("OUR WINDOW :" + z)
          val producer1 = new KafkaProducer[String, String](props)
          var msg = b._1+","+z.toString+","+(z-window_duration).toString +"," +b._3.toString
          println(msg)
          val record1:ProducerRecord[String, String] = new ProducerRecord("test2", msg)
          producer1.send(record1)
          // WindowData += b._1+","+z.toString+","+(z-window_duration).toString +"," +b._3.toString
          //var f = dateFormat.parse
//          var f = dateFormat.format(z)
//          var l = dateFormat.format(z-window_duration)
//          var speed = b._3
//          var imei =  b._1
//          val table =ssc.cassandraTable(CASSANDRA_KEYSPACE,CASSANDRA_TABLE1)
//          table.foreach(c=>println("TABLE DATA :" + c))
//          val filteredtable =  table.select("speed").where("imei=?", imei).where("unprocessedcreated > ?",f).where("unprocessedcreated < ?",l)
//          filteredtable.foreach(c=>println("FILTERED TABLE DATA :" + c))
//          val avg_map = filteredtable.map(x=>(x.dataAsString.toFloat,1)).reduce((a,b)=>((a._1.toFloat + b._1.toFloat ).toFloat,((b._2 +a._2).toString.toInt)))
//          val new_avg = (avg_map._1 +speed) /(avg_map._2 + 1)
//          println("NEW AVERAGE :" + new_avg)
//
//        })
      })
    })}))
//    WindowData.foreach(g=>{
//      println("WINDOW DATA :" + g)
//     var imei = g.split(",")(0)
//      var f = g.split(",")(2)
//      var l = g.split(",")(1)
//      var s= g.split(",")(3).toFloat
//      val table = ssc.cassandraTable(CASSANDRA_KEYSPACE,CASSANDRA_TABLE1)
//      table.foreach(c=>println("TABLE DATA :" + c))
//     val filteredtable =  table.select("speed").where("imei=?", imei).where("unprocessedcreated > ?",f).where("unprocessedcreated < ?",l)
//      filteredtable.foreach(c=>println("FILTERED TABLE DATA :" + c))
//      val avg_map = filteredtable.map(x=>(x.dataAsString.toFloat,1)).reduce((a,b)=>((a._1.toFloat + b._1.toFloat ).toFloat,((b._2 +a._2).toString.toInt)))
//       val new_avg = (avg_map._1 +s) /(avg_map._2 + 1)
//      println("NEW AVERAGE :" + new_avg)
////      val query = "update sample set avgspeed = "+ new_avg +"where windowtime = " + last+";"
////      val KeySpace    = new CassandraSQLContext(ssc)
////      KeySpace.setKeyspace(CASSANDRA_KEYSPACE)
////
////      hourUniqueKeySpace.sql(query)
//
//   })
   // WindowData.foreach(g=>println(g))

   // table.foreach(x=>println(x))
    val avgFn = (x:(Float, Int), y: (Float, Int)) => (x._1 + y._1, x._2 + y._2)
    val speedData = cachedStream.map(x => (x._1, x._3)).mapValues(s => (s, 1))
    val averageSpeed = speedData.reduceByKeyAndWindow(avgFn,Seconds(window_duration),Seconds(window_duration))
      .mapValues((x) => x._1 / x._2)
      .map(x => (x._1, x._2))
    averageSpeed.foreachRDD((rdd,time)=> {
      window_time = (time.milliseconds/1000).toString
      // e = (time.milliseconds/1000).toString
      WindowList += time.milliseconds/1000

    }
    )
    //averageSpeed.foreachRDD(x=>x.foreach(a=>println(a)))

    //WindowList.foreach(c=>DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").print(c))
    // val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    // var new_date = dateFormat.
    //val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
   //  val id = UUIDs.timeBased()
    averageSpeed.map(a=>(UUIDs.timeBased(),a._1,a._2,dateFormat.format(window_time.toLong*1000l))).saveToCassandra(CASSANDRA_KEYSPACE, CASSANDRA_TABLE2, CASSANDRA_COLUMNS2)
    parsedLine.map(x=>(UUIDs.timeBased(),x._1,x._2,x._3,x._4,x._5))saveToCassandra(CASSANDRA_KEYSPACE, CASSANDRA_TABLE1, CASSANDRA_COLUMNS1)

    //println(parsedLine)
    //val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")


    val overSpeedData = parsedLine.filter(s => s._3 >= SPEED_LIMIT)
//    val props = new Properties()
//    props.put("bootstrap.servers", KAFKA_SERVERS)
//    props.put("client.id", "Producer")
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
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


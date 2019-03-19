package com.gadgeon.koreone

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import org.apache.spark.sql.SparkSession
import com.datastax.driver.core.utils.UUIDs
//import org.apache.spark.sql.cassandra.CassandraSQLContext
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._

object Recalculation {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)
    val KAFKA_SERVERS = "localhost:9092"
    val INPUT_TOPIC = "test2"
    val CASSANDRA_HOST = "localhost"
    val CASSANDRA_KEYSPACE = "koreone"
    val CASSANDRA_TABLE1 = "speed"
    val CASSANDRA_TABLE2 = "sample"
    val CHECKPOINT_PATH = "c:\\tmp"
    val CASSANDRA_COLUMNS2= SomeColumns("id","imei","avgspeed","windowendtime")
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Recalculation")
      .set("spark.cassandra.connection.host", CASSANDRA_HOST)
    val ssc = new StreamingContext(conf, Seconds(1))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> KAFKA_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(INPUT_TOPIC)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val message = stream.map(_.value)
    val parsed_message = message.map(m=>{
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      var splits = m.split(",")
      var imei = splits(0)
      var end  = dateFormat.format(splits(1).toLong * 1000l)
      var start = dateFormat.format(splits(2).toLong * 1000l)
      var speed = splits(3).toFloat
      (imei,speed,start,end)

    })
    parsed_message.foreachRDD(rdd=> rdd.foreachPartition(partition=>partition.foreach(m=>{
                   println(m)
      val spark = SparkSession.builder.config(conf).getOrCreate()
          @transient var table =spark.sparkContext.cassandraTable[Float](CASSANDRA_KEYSPACE,CASSANDRA_TABLE1)
             .select("speed").where("imei=?", m._1)//.collect
             .where("unprocessedcreateddate > ?",m._3)
             .where("unprocessedcreateddate < ?",m._4)
           table.foreach(c=>println("TABLE DATA :" + c))
//                val filteredtable =  table.select("speed").where("imei=?", m._1).where("unprocessedcreated > ?",m._3).where("unprocessedcreated < ?",m._4)
                //table.foreach(c=>println("FILTERED TABLE DATA :" + c))
                var avg_map :(Float,Int) = table.map(x=>(x,1)).reduce((a,b)=>((a._1 + b._1 ),((b._2 +a._2))))
                var new_avg : Float = (avg_map._1 + m._2) / (avg_map._2 + 1)
                println("NEW AVERAGE :" + new_avg)
      val table2 = spark.sparkContext.cassandraTable[(java.util.UUID,String)](CASSANDRA_KEYSPACE,CASSANDRA_TABLE2)
        .select("id","imei").where("windowendtime = ?",m._4).where("imei = ?",m._1)
     table2.map(g=>(g._1,m._1,new_avg,m._4)).saveToCassandra(CASSANDRA_KEYSPACE, CASSANDRA_TABLE2, CASSANDRA_COLUMNS2)

    })))
    ssc.checkpoint(CHECKPOINT_PATH)
    ssc.start()
    ssc.awaitTermination()
  }

}

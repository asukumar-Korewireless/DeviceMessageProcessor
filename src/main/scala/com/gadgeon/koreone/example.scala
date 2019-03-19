package com.gadgeon.koreone

import java.util.{Calendar, Date, TimeZone}

import com.datastax.driver.core.utils.UUIDs
//import org.apache.spark.sql.cassandra.CassandraSQLContext
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import com.gadgeon.koreone.PortwiseProcess.dateFormat
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

object example {

  def main(args: Array[String]): Unit = {
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
   // dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
    val conf = new SparkConf().setAppName("Example").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val table = sc.cassandraTable("koreone","speed").select("speed").where("imei=?", "AA001")//.where("unprocessedcreated > ?",f).where("unprocessedcreated < ?",l)
    table.foreach(c=>println(c))
//   var d : Date  = new Date(1552804902000l)
//    println(dateFormat.format(d))
  }

}

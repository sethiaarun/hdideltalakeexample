package com.ms.hdi.spark.deltalake.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * delta table example with Spark 2.4 and HDI 4.0
 */
object DeltaLakeExample extends App {

  val sparkConf = new SparkConf()
  // we need to unset this spark.sql.queryExecutionListeners to avoid any exception. These errors are harmless.
  // java.lang.NullPointerException
  //at com.microsoft.peregrine.spark.listeners.PlanLogListener$2.apply(PlanLogListener.java:180)
  sparkConf.set("spark.sql.queryExecutionListeners", "")
  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .appName("TestDataLake").getOrCreate()
  val numOfRec = 1000
  // write using delta lake:q!:
  spark.range(numOfRec).repartition(1).write.mode("overwrite").format("delta").save("/tmp/deltademodata")
  // read using delta lake
  val df =spark.read.format("delta").load("/tmp/deltademodata")
  println(s"Number of records written ${df.count()}")
  //read using delta table
  import io.delta.tables._
  val dt: io.delta.tables.DeltaTable = DeltaTable.forPath("/tmp/deltademodata")
  //show available commits
  println("available commits")
  dt.history().show(false)
  //add additional data
  // append additional 100 records, it should create another version
  spark.range(numOfRec,numOfRec+100).repartition(1).write.mode("overwrite").format("delta").save("/tmp/deltademodata")
  //show available commits
  println("available commits after append data")
  dt.history().show(false)
  // read version zero - time travel
  val dfVersionZero=spark.read.format("delta").option("versionAsOf",0).load("/tmp/deltademodata")
  println(s"version zero, number of records ${dfVersionZero.count()}")
  //delta table auto refresh
  println(s"Number of records after append ${dt.toDF.count()}")
}

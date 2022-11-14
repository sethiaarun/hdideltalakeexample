package com.ms.hdi.spark.deltalake.example

import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * delta table example with Spark 3 and HDI 5.0
 */
object DeltaLakeSpark3Example extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  val adsl2Path = "/tmp/deltadexample"
  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .appName("TestDataLake").getOrCreate()
  val numOfRec = 1000

  // write using delta lake:q!:
  spark.range(numOfRec)
    .repartition(1)
    .write
    .mode("overwrite")
    .format("delta")
    .save(adsl2Path)

  // read using delta lake
  val df = spark.read.format("delta").load(adsl2Path)
  println(s"Number of records written ${df.count()}")

  //read using delta table
  val dt: io.delta.tables.DeltaTable = DeltaTable.forPath(adsl2Path)

  //show available commits
  println("available commits")
  dt.history().show(false)

  //add additional data
  // append additional 100 records, it should create another version
  spark.range(numOfRec, numOfRec + 100)
    .repartition(1)
    .write
    .mode("overwrite")
    .format("delta")
    .save(adsl2Path)

  //show available commits
  println("available commits after append data")
  dt.history().show(false)

  // read version one - time travel
  val dfVersionOne = spark.read.format("delta").option("versionAsOf", 1).load(adsl2Path)
  println(s"version zero, number of records ${dfVersionOne.count()}")

  //delta table auto refresh
  println(s"Number of records after append ${dt.toDF.count()}")
}
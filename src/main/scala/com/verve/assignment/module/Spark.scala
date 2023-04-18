package com.verve.assignment.module

import org.apache.spark.sql.SparkSession

object Spark {
  def session(): SparkSession = {
    SparkSession.builder()
      .master("spark-master:7077")
      .appName("Spark SQL basic example")
//      .config("spark.sql.warehouse.dir", getClass.getResource("/").getPath)
      .getOrCreate()
  }
}

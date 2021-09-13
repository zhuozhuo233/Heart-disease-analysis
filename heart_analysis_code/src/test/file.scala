package test

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession

object file {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


  }
}
package test

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object cpprocess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val heart = spark.read.table("heart.heart")

    val cp = heart.select("age","cp","target")
    cp.write.mode("overwrite").json("/user/root/heart/age_cp_target").


  }
}
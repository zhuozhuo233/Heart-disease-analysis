package test

import org.apache.spark.sql.SparkSession

object exangprocess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val heart = spark.read.table("heart.heart")

    val exang = heart.select("exang","target")
    exang.write.mode("overwrite").json("/user/root/heart/exang_target")


  }

}

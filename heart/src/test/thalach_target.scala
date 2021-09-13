package test

import org.apache.spark.sql.SparkSession

object thalach_target {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val heart = spark.read.table("heart.heart")

    val thalach = heart.select("thalach","target")
    thalach.write.mode("overwrite").json("/user/root/heart/thalach_target")


  }

}

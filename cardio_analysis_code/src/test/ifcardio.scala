package test

import org.apache.spark.sql.SparkSession

object ifcardio {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cardio = spark.read.table("cardio.cardio3")

    val cardio1 = cardio.select("age", "cardio")
    cardio1.write.mode("overwrite").json("/user/root/cardio/age_cardio")
  }

}

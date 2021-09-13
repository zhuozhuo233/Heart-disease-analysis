package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spire.random.Random.float

object ageprocess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val heart = spark.read.table("heart.heart")

//    val age29_40 = heart.filter("age >= '29' and age <= '40'").show()
    val age29_40_1 = heart.filter("age >= '29' and age <= '40'").filter("target = '1'").count()
    val age29_40_2 = heart.filter("age >= '29' and age <= '40'").count()
    val result29_40 = (age29_40_1.toFloat/age29_40_2).formatted("%.2f").toFloat
    println("29到40岁患病风险:")
    baifenshu(result29_40)

    val age41_50_1 = heart.filter("age >= '41' and age <= '50'").filter("target = '1'").count()
    val age41_50_2 = heart.filter("age >= '41' and age <= '50'").count()
    val result41_50 = (age41_50_1.toFloat/age41_50_2).formatted("%.2f").toFloat
    println("41到50岁患病风险:")
    baifenshu(result41_50)

    val age51_60_1 = heart.filter("age >= '51' and age <= '60'").filter("target = '1'").count()
    val age51_60_2 = heart.filter("age >= '51' and age <= '60'").count()
    val result51_60 = (age51_60_1.toFloat/age51_60_2).formatted("%.2f").toFloat
    println("51到60岁患病风险:")
    baifenshu(result51_60)

    val age61_70_1 = heart.filter("age >= '61' and age <= '70'").filter("target = '1'").count()
    val age61_70_2 = heart.filter("age >= '61' and age <= '70'").count()
    val result61_70 = (age61_70_1.toFloat/age61_70_2).formatted("%.2f").toFloat
    println("61到70岁患病风险:")
    baifenshu(result61_70)

    val age71_77_1 = heart.filter("age >= '71' and age <= '77'").filter("target = '1'").count()
    val age71_77_2 = heart.filter("age >= '71' and age <= '77'").count()
    val result71_77 = (age71_77_1.toFloat/age71_77_2).formatted("%.2f").toFloat
    println("70到71岁患病风险:")
    baifenshu(result71_77)




  }

  def baifenshu(x: Float ): Unit ={
    var per = (x * 100).toInt
    println(per + "%")


  }


}
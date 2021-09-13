package test

import org.apache.spark.sql.SparkSession

object ageprocess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cardio = spark.read.table("cardio.cardio")

    cardio.selectExpr("max(age) as max_age").show()
    cardio.selectExpr("min(age) as min").show()

    val agetotal = cardio.select("age").count()


    val age30_35 = cardio.filter("age >= '30' and age <='35'").filter("cardio = '1'").count()
    val result30_35 = (age30_35.toFloat/agetotal).formatted("%.2f").toFloat
    println("30到35岁比率：")
    rate(result30_35)

    val age36_40 = cardio.filter("age >= '36' and age <='40'").filter("cardio = '1'").count()
    val result30_40 = (age36_40.toFloat/agetotal).formatted("%.2f").toFloat
    println("36到40岁比率：")
    rate(result30_40)

    val age41_45 = cardio.filter("age >= '41' and age <='45'").filter("cardio = '1'").count()
    val result41_45 = (age41_45.toFloat/agetotal).formatted("%.2f").toFloat
    println("41到45岁比率：")
    rate(result41_45)

    val age46_50 = cardio.filter("age >= '46' and age <='50'").filter("cardio = '1'").count()
    val result46_50 = (age46_50.toFloat/agetotal).formatted("%.2f").toFloat
    println("46到50岁比率：")
    rate(result46_50)

    val age51_55 = cardio.filter("age >= '51' and age <='55'").filter("cardio = '1'").count()
    val result51_55 = (age51_55.toFloat/agetotal).formatted("%.2f").toFloat
    println("51到55岁比率：")
    rate(result51_55)

    val age56_60 = cardio.filter("age >= '56' and age <='60'").filter("cardio = '1'").count()
    val result56_60 = (age56_60.toFloat/agetotal).formatted("%.2f").toFloat
    println("56到60岁比率：")
    rate(result56_60)

    val age61_65 = cardio.filter("age >= '61' and age <='65'").filter("cardio = '1'").count()
    val result61_65 = (age61_65.toFloat/agetotal).formatted("%.2f").toFloat
    println("60到65岁比率：")
    rate(result61_65)

  }
  def rate(x:Float):Unit ={
    var per = (x * 100).toInt
    println(per)
  }
}

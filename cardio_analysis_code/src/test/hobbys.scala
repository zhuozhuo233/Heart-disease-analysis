package test

import org.apache.spark.sql.SparkSession

object hobbys {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cardio = spark.read.table("cardio.cardio")

    val smoke = cardio.select("smoke").filter("cardio = '1'").count()
    val issmoke = cardio.select("smoke").filter("smoke = '1'").filter("cardio = '1'").count()
    val smokeresult = (issmoke.toFloat/smoke).formatted("%.2f").toFloat
    println("心脏病患者中有吸烟习惯的比例:")
    rate(smokeresult)

    val alco = cardio.select("alco").filter("cardio = '1'").count()
    val isalco = cardio.select("alco").filter("alco = '1'").filter("cardio = '1'").count()
    val alcoresult = (isalco.toFloat/alco).formatted("%.2f").toFloat
    println("心脏病患者中有饮酒习惯的比例:")
    rate(alcoresult)

    val active = cardio.select("active").filter("cardio = '1'").count()
    val isactive = cardio.select("active").filter("active = '1'").filter("cardio = '1'").count()
    val activeresult = (isactive.toFloat/active).formatted("%.2f").toFloat
    println("心脏病患者中有锻炼习惯的比例:")
    rate(activeresult)



  }
  def rate(x:Float):Unit ={
    var per = (x * 100).toInt
    println(per)
  }

}

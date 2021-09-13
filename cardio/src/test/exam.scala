package test

import org.apache.spark.sql.SparkSession

object exam {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cardio = spark.read.table("cardio.cardio")

    val cho = cardio.select("cholesterol").filter("cardio = '1'").count()
    val cho1 = cardio.select("cholesterol").filter("cholesterol = '1'").filter("cardio = '1'").count()
    val cho2 = cardio.select("cholesterol").filter("cholesterol = '2'").filter("cardio = '1'").count()
    val cho3 = cardio.select("cholesterol").filter("cholesterol = '3'").filter("cardio = '1'").count()

    val smokeresult1 = (cho1.toFloat/cho).formatted("%.2f").toFloat
    println("心脏病患者中胆固醇数值等于正常的比例:")
    rate(smokeresult1)

    val smokeresult2 = (cho2.toFloat/cho).formatted("%.2f").toFloat
    println("心脏病患者中胆固醇数值高于正常的比例:")
    rate(smokeresult2)

    val smokeresult3 = (cho3.toFloat/cho).formatted("%.2f").toFloat
    println("心脏病患者中胆固醇数值远高于正常的比例:")
    rate(smokeresult3)

    val gluc = cardio.select("gluc").filter("cardio = '1'").count()
    val gluc1 = cardio.select("gluc").filter("gluc = '1'").filter("cardio = '1'").count()
    val gluc2 = cardio.select("gluc").filter("gluc = '2'").filter("cardio = '1'").count()
    val gluc3 = cardio.select("gluc").filter("gluc = '3'").filter("cardio = '1'").count()

    val glucresult1 = (gluc1.toFloat/gluc).formatted("%.2f").toFloat
    println("心脏病患者中血糖数值等于正常的比例:")
    rate(glucresult1)

    val glucresult2 = (gluc2.toFloat/gluc).formatted("%.2f").toFloat
    println("心脏病患者中血糖数值高于正常的比例:")
    rate(glucresult2)

    val glucresult3 = (gluc3.toFloat/gluc).formatted("%.2f").toFloat
    println("心脏病患者中血糖数值远高于正常的比例:")
    rate(glucresult3)




  }
  def rate(x:Float):Unit ={
    var per = (x * 100).toInt
    println(per)
  }


}

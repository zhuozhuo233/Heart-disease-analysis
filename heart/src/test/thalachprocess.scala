package test

import org.apache.spark.sql.SparkSession

object thalachprocess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val heart = spark.read.table("heart.heart")

    //    val age29_40 = heart.filter("age >= '29' and age <= '40'").show()
    val thalach71_90_1 = heart.filter("thalach >= '71' and thalach <= '90'").filter("target = '1'").count()
    val thalach71_90_2 = heart.filter("thalach >= '71' and thalach <= '90'").count()
    val thalach71_90 = (thalach71_90_1.toFloat/thalach71_90_2).formatted("%.2f").toFloat
    println("最高心率在71到90每分钟的患病概率:")
    baifenshu(thalach71_90)
    println("最高心率在71到90每分钟的统计人数：")
    println(thalach71_90_2)


    val thalach91_110_1 = heart.filter("thalach >= '91' and thalach <= '110'").filter("target = '1'").count()
    val thalach91_110_2 = heart.filter("thalach >= '91' and thalach <= '110'").count()
    val thalach91_110 = (thalach91_110_1.toFloat/thalach91_110_2).formatted("%.2f").toFloat
    println("最高心率在91到110每分钟的患病概率:")
    baifenshu(thalach91_110)
    println("最高心率在91到110每分钟的统计人数：")
    println(thalach91_110_2)

    val thalach111_130_1 = heart.filter("thalach >= '111' and thalach <= '130'").filter("target = '1'").count()
    val thalach111_130_2 = heart.filter("thalach >= '111' and thalach <= '130'").count()
    val thalach111_130 = (thalach111_130_1.toFloat/thalach111_130_2).formatted("%.2f").toFloat
    println("最高心率在111到130每分钟的患病概率:")
    baifenshu(thalach111_130)
    println("最高心率在111到130每分钟的统计人数：")
    println(thalach111_130_2)

    val thalach131_150_1 = heart.filter("thalach >= '131' and thalach <= '150'").filter("target = '1'").count()
    val thalach131_150_2 = heart.filter("thalach >= '131' and thalach <= '150'").count()
    val thalach131_150 = (thalach131_150_1.toFloat/thalach131_150_2).formatted("%.2f").toFloat
    println("最高心率在131到150每分钟的患病概率:")
    baifenshu(thalach131_150)
    println("最高心率在131到150每分钟的统计人数：")
    println(thalach131_150_2)

    val thalach151_170_1 = heart.filter("thalach >= '151' and thalach <= '170'").filter("target = '1'").count()
    val thalach151_170_2 = heart.filter("thalach >= '151' and thalach <= '170'").count()
    val thalach151_170 = (thalach151_170_1.toFloat/thalach151_170_2).formatted("%.2f").toFloat
    println("最高心率在151到170每分钟的患病概率:")
    baifenshu(thalach151_170)
    println("最高心率在151到170每分钟的统计人数：")
    println(thalach151_170_2)

    val thalach171_190_1 = heart.filter("thalach >= '171' and thalach <= '190'").filter("target = '1'").count()
    val thalach171_190_2 = heart.filter("thalach >= '171' and thalach <= '190'").count()
    val thalach171_190 = (thalach171_190_1.toFloat/thalach171_190_2).formatted("%.2f").toFloat
    println("最高心率在171到190每分钟的患病概率:")
    baifenshu(thalach171_190)
    println("最高心率在171到190每分钟的统计人数：")
    println(thalach171_190_2)

    val thalach191_202_1 = heart.filter("thalach >= '191' and thalach <= '202'").filter("target = '1'").count()
    val thalach191_202_2 = heart.filter("thalach >= '191' and thalach <= '202'").count()
    val thalach191_202 = (thalach191_202_1.toFloat/thalach171_190_2).formatted("%.2f").toFloat
    println("最高心率在191到202每分钟的患病概率:")
    baifenshu(thalach191_202)
    println("最高心率在191到202每分钟的统计人数：")
    println(thalach191_202_2)



  }

  def baifenshu(x: Float ): Unit ={
    var per = (x * 100).toInt
    println(per + "%")


  }


}

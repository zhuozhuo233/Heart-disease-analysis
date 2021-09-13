package test

import org.apache.spark.sql.SparkSession


object ap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cardio = spark.read.table("cardio.cardio")

    val ap_hi = cardio.select("ap_hi").filter("cardio = '1'").count()
    val ap_hi_1 = cardio.select("ap_hi").filter("cardio = '1'").filter("ap_hi >= '90' and ap_hi <= '140'").count()
    val ap_hiresult = (ap_hi_1.toFloat/ap_hi).formatted("%.2f").toFloat
    println("心脏病患者中收缩压正常的比率:")
    rate(ap_hiresult)

    val ap_lo = cardio.select("ap_lo").filter("cardio = '1'").count()
    val ap_lo_1 = cardio.select("ap_lo").filter("cardio = '1'").filter("ap_lo >= '60' and ap_hi <= '90'").count()
    val ap_loresult = (ap_lo_1.toFloat/ap_lo).formatted("%.2f").toFloat
    println("心脏病患者中舒张压正常的比率:")
    rate(ap_loresult)

    val ap_hi100_110 = cardio.filter("cardio = '1'").filter("ap_hi >= '100' and ap_hi <='110'").count()
    val result100_110 = (ap_hi100_110.toFloat/ap_hi).formatted("%.2f").toFloat
    println("收缩压在100~110mmHg：")
    rate(result100_110)

    val ap_hi110_120 = cardio.filter("cardio = '1'").filter("ap_hi >= '111' and ap_hi <='120'").count()
    val result110_120 = (ap_hi110_120.toFloat/ap_hi).formatted("%.2f").toFloat
    println("收缩压在110~120mmHg：")
    rate(result110_120)

    val ap_hi120_130 = cardio.filter("cardio = '1'").filter("ap_hi >= '121' and ap_hi <='130'").count()
    val result120_130 = (ap_hi120_130.toFloat/ap_hi).formatted("%.2f").toFloat
    println("收缩压在120~130mmHg：")
    rate(result120_130)

    val ap_hi130_140 = cardio.filter("cardio = '1'").filter("ap_hi >= '131' and ap_hi <='140'").count()
    val result130_140 = (ap_hi130_140.toFloat/ap_hi).formatted("%.2f").toFloat
    println("收缩压在130~140mmHg：")
    rate(result130_140)

    val ap_hi140_150 = cardio.filter("cardio = '1'").filter("ap_hi >= '141' and ap_hi <='150'").count()
    val result140_150 = (ap_hi140_150.toFloat/ap_hi).formatted("%.2f").toFloat
    println("收缩压在140~150mmHg：")
    rate(result140_150)

    val ap_hi150_160 = cardio.filter("cardio = '1'").filter("ap_hi >= '151' and ap_hi <='160'").count()
    val result150_160 = (ap_hi150_160.toFloat/ap_hi).formatted("%.2f").toFloat
    println("收缩压在150~160mmHg：")
    rate(result150_160)

    val ap_hi160_170 = cardio.filter("cardio = '1'").filter("ap_hi >= '161' and ap_hi <='170'").count()
    val result160_170 = (ap_hi160_170.toFloat/ap_hi).formatted("%.2f").toFloat
    println("收缩压在160~170mmHg：")
    rate(result160_170)

    val ap_hi170_180 = cardio.filter("cardio = '1'").filter("ap_hi >= '171' and ap_hi <='180'").count()
    val result170_180 = (ap_hi170_180.toFloat/ap_hi).formatted("%.2f").toFloat
    println("收缩压在170~180mmHg：")
    rate(result170_180)

    val ap_hi180_190 = cardio.filter("cardio = '1'").filter("ap_hi >= '181' and ap_hi <='190'").count()
    val result180_190 = (ap_hi180_190.toFloat/ap_hi).formatted("%.2f").toFloat
    println("收缩压在180~190mmHg：")
    rate(result180_190)

    val ap_hi190_200 = cardio.filter("cardio = '1'").filter("ap_hi >= '191' and ap_hi <='200'").count()
    val result190_200 = (ap_hi190_200.toFloat/ap_hi).formatted("%.2f").toFloat
    println("收缩压在190~200mmHg：")
    rate(result190_200)
//   ----------------舒张压-------------------------------
    val ap_lo60_70 = cardio.filter("cardio = '1'").filter("ap_lo >= '60' and ap_lo <='70'").count()
    val result60_70 = (ap_lo60_70.toFloat/ap_lo).formatted("%.2f").toFloat
    println("舒张压在60~70mmHg：")
    rate(result60_70)

    val ap_lo70_80 = cardio.filter("cardio = '1'").filter("ap_lo >= '71' and ap_lo <='80'").count()
    val result70_80 = (ap_lo70_80.toFloat/ap_lo).formatted("%.2f").toFloat
    println("舒张压在70~80mmHg：")
    rate(result70_80)

    val ap_lo80_90 = cardio.filter("cardio = '1'").filter("ap_lo >= '81' and ap_lo <='90'").count()
    val result80_90 = (ap_lo80_90.toFloat/ap_lo).formatted("%.2f").toFloat
    println("舒张压在80~90mmHg：")
    rate(result80_90)

    val ap_lo90_100 = cardio.filter("cardio = '1'").filter("ap_lo >= '91' and ap_lo <='100'").count()
    val result90_100 = (ap_lo90_100.toFloat/ap_lo).formatted("%.2f").toFloat
    println("舒张压在90~100mmHg：")
    rate(result90_100)

    val ap_lo100_110 = cardio.filter("cardio = '1'").filter("ap_lo >= '101' and ap_lo <='110'").count()
    val resultlo100_110 = (ap_lo100_110.toFloat/ap_lo).formatted("%.2f").toFloat
    println("舒张压在100~110mmHg：")
    rate(resultlo100_110)

    val ap_lo110_120 = cardio.filter("cardio = '1'").filter("ap_lo >= '111' and ap_lo <='120'").count()
    val resultlo110_120 = (ap_lo110_120.toFloat/ap_lo).formatted("%.2f").toFloat
    println("舒张压在110~120mmHg：")
    rate(resultlo110_120)







  }

  def rate(x:Float):Unit ={
    var per = (x * 100).toInt
    println(per)
  }

}

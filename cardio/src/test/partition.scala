package test
import org.apache.spark.sql.SparkSession

object partition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cardio = spark.read.table("cardio.cardio")

    val id = cardio.select("id")
    id.write.mode("overwrite").json("/user/root/cardio/id")

    val age = cardio.select("age")
    age.write.mode("overwrite").json("/user/root/cardio/age")

    val gender = cardio.select("gender")
    gender.write.mode("overwrite").json("/user/root/cardio/gender")

    val ap_hi = cardio.select("ap_hi")
    ap_hi.write.mode("overwrite").json("/user/root/cardio/ap_hi")

    val ap_lo = cardio.select("ap_lo")
    ap_lo.write.mode("overwrite").json("/user/root/cardio/ap_lo")

    val cholesterol = cardio.select("cholesterol")
    cholesterol.write.mode("overwrite").json("/user/root/cardio/cholesterol")

    val gluc = cardio.select("gluc")
    gluc.write.mode("overwrite").json("/user/root/cardio/gluc")

    val smoke = cardio.select("smoke")
    smoke.write.mode("overwrite").json("/user/root/cardio/smoke")

    val alco = cardio.select("alco")
    alco.write.mode("overwrite").json("/user/root/cardio/alco")

    val active = cardio.select("active")
    active.write.mode("overwrite").json("/user/root/cardio/active")

    val cardio1 = cardio.select("cardio")
    cardio1.write.mode("overwrite").json("/user/root/cardio/cardio1")




  }

}

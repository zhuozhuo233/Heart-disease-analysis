package test

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object age {
  def main(args:Array[String]): Unit = {
    val spark=SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

//  读取前端保存到数据库的表信息文件
    val heart = spark.read.table("heart.heart2")

//    所有标签设置为常量
    val age = heart.select("age")
//    age.show()
    val sex = heart.select("sex")
//    sex.show()
    val cp = heart.select("cp")
//    cp.show()
    val trestbps = heart.select("trestbps")
//    trestbps.show()
    val chol = heart.select("chol")
//    chol.show()
    val fbs = heart.select("fbs")
//    fbs.show()
    val restecg = heart.select("restecg")
//    restecg.show()
    val thalach = heart.select("thalach")
//    thalach.show()
    val exang = heart.select("exang")
//    exang.show()
    val oldpeak = heart.select("oldpeak")
//    oldpeak.show()
    val slope = heart.select("slope")
//    slope.show()
    val ca = heart.select("ca")
//    ca.show()
    val thal = heart.select("thal")
//    thal.show()
    val target = heart.select("target")
//    target.show()

//机器学习输入上述常量与大数据模型进行对比，返回一个常量user_target

//将返回的常量user_target以json格式保存到hdfs供前端读取
    target.write.mode("overwrite").json("/user/root/heart")
//  user_target.write.mode("overwrite").json("/user/root/heart")

  }

}

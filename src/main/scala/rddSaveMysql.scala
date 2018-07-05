// makeRDD
// RDD to Dataframe
// Dataframe Save to MySQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.Properties
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object rddSaveMysql extends App {

  val appName = "rddSaveMySQL"
  val master = "local[*]"
  val conf = new SparkConf().setAppName(appName).setMaster(master)

  val sc = SparkSession.builder
    .master(master)
    .appName(appName)
    .config("spark.some.config.option", "config-value")
    .getOrCreate()

  // toDF method를 사용할 경우 아래 import
  import sc.implicits._

  val values = List("20180705", 1.0)

  // Row 생성
  val row = Row.fromSeq(values)

  // SparkSession은 2.x 이후 엔트리 포인트로, 내부에 sparkContext를 가지고 있음
  val rdd = sc.sparkContext.makeRDD(List(row))

  val fields = List(
    StructField("First Column", StringType, nullable = false),
    StructField("Second Column", DoubleType, nullable = false)
  )

  val dataFrame = sc.createDataFrame(rdd, StructType(fields))

  val properties = new Properties()
  properties.put("user", "mysql_username")
  properties.put("password", "your_mysql_password")

  // db column은 fields에서 정의
  dataFrame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/dbtest", "test", properties)

  println("Dataframe Save to MySQL!")
}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object sparkSQL extends App {
  case class Person(name: String, age: Int)

  val appName = "sparkSQL"
  val master = "local[*]"
  val conf = new SparkConf().setAppName(appName).setMaster(master)

  val sc = SparkSession.builder
    .master(master)
    .appName(appName)
    .config("spark.some.config.option", "config-value")
    .getOrCreate()

  import sc.implicits._
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  val peopleRDD: RDD[Person] = sc.sparkContext.parallelize(Seq(Person("kyle", 10), Person("Seongyun", 29)))

  val people = peopleRDD.toDS
  println("Total Data")
  people.show()
//  +--------+---+
//  |    name|age|
//  +--------+---+
//  |    kyle| 10|
//  |Seongyun| 29|
//  +--------+---+

  // Dataset Query
  val teenagers = people.where('age >= 10).where('age <= 19).select('name).as[String]
  println("Dataset Query Part")
  teenagers.show()
//  +----+
//  |name|
//  +----+
//  |kyle|
//  +----+


  // Create Temp View
  // [ds or df].createOrReplaceTempView(viewname='value')
  // Zeppelin에선 이렇게 생성한 후, %sql 쿼리문으로 사용 가능

  people.createOrReplaceTempView("people")

  val teenagers2 = sc.sql("SELECT * FROM people WHERE age >= 10 AND age <= 19")
  println("SQL Query Part")
  teenagers2.show()
//  +----+---+
//  |name|age|
//  +----+---+
//  |kyle| 10|
//  +----+---+

}

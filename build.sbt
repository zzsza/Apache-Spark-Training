lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "zzsza",
    libraryDependencies ++= List(
      "org.twitter4j" % "twitter4j-core" % "4.0.6",
      "org.apache.bahir" % "spark-streaming-twitter_2.11" % "2.1.0",
      "org.apache.spark" % "spark-core_2.11" % "2.2.0",
      "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
      "mysql" % "mysql-connector-java" % "5.1.12"
    ),
    retrieveManaged := true
  )

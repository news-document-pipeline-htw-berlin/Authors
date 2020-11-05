
name := "Authors"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0")


// SparkMongo https://docs.mongodb.com/spark-connector/master/scala-api
libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.0")


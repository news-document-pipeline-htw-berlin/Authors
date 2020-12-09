import authorMapping.{Authors, Scoring}
import db.DBConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

object App {

  /*
   Joins two SQL Dataframes by column _id
  */
  def joinDataFrames(frame1: DataFrame, frame2: DataFrame): DataFrame = {
    frame1.join(frame2, Seq("_id"))
  }

  /*
   Reads connection settings from db files in resources
  */

  def getConnectionInfoFromFile(pathToFile: String): Map[String, String] = {
    val bufferedSource = scala.io.Source.fromFile(pathToFile)

    val map = bufferedSource.mkString // turn it into one long String
      .split("(?=\\n\\S+\\s*->)") // a non-consuming split
      .map(_.trim.split("\\s*->\\s*")) // split each element at "->"
      .map(arr => arr(0) -> arr(1)) // from 2-element Array to tuple
      .toMap
    bufferedSource.close()

    map
  }


  def main(args: Array[String]): Unit = {
    val inputMap = getConnectionInfoFromFile("src/main/resources/inputDBSettings")
    val outputMap = getConnectionInfoFromFile("src/main/resources/outputDBSettings")

    val inputUri = DBConnector.createUri(inputMap.getOrElse("inputUri", throw new IllegalArgumentException),
      inputMap.getOrElse("inputDB", throw new IllegalArgumentException),
      inputMap.getOrElse("inputCollection", throw new IllegalArgumentException))

    val outputUri = DBConnector.createUri(outputMap.getOrElse("outputUri", throw new IllegalArgumentException),
      outputMap.getOrElse("outputDB", throw new IllegalArgumentException),
      outputMap.getOrElse("outputCollection", throw new IllegalArgumentException))


    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Author analysis")
      .config("spark.mongodb.input.uri", inputUri)
      .config("spark.mongodb.output.uri", outputUri)
      .getOrCreate()


    val readConfig = DBConnector.createReadConfig(inputUri, spark)
    val writeConfig = DBConnector.createWriteConfig(outputUri, sparkSession = spark)
    val mongoData = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfig)

    // Mapping elements
    val groupedAuthors = Authors.groupByAuthorRDDRow(mongoData)
    val amountOfSourcesPerAuthor = Authors.averageSourcesPerAuthor(groupedAuthors)
    val publishedOnDay = Authors.publishedOnDayRDD(groupedAuthors)
    val perWebsite = Authors.amountOfArticlesByWebsiteRDD(groupedAuthors)
    val averageWordsPerArticle = Authors.averageWordsPerArticleRDD(groupedAuthors)
    val amountOfArticles = Authors.amountOfArticlesPerAuthor(groupedAuthors)
    val perDepartment = Authors.articlesPerDepartment(groupedAuthors)
    val lastTexts = Authors.lastNTexts(groupedAuthors,5)

    // Creation of Dataframes
    val articles = spark.createDataFrame(amountOfArticles.collect()).toDF("_id", "articles")
    val averageWords = spark.createDataFrame(averageWordsPerArticle.collect()).toDF("_id", "averageWords")
    val daysPublished = spark.createDataFrame(publishedOnDay.collect()).toDF("_id", "daysPublished")
    val perWebsiteDF = spark.createDataFrame(perWebsite.collect()).toDF("_id", "perWebsite")
    val perDepartmentDF = spark.createDataFrame(perDepartment.collect()).toDF("_id", "perDepartment")
    val amountSourceDF = spark.createDataFrame(amountOfSourcesPerAuthor.collect()).toDF("_id", "avgAmountOfSources")
    val lastTextsDF = spark.createDataFrame(lastTexts).toDF("_id", "lastTexts")


    // Trust score for authors
    val defaultScore = Scoring.giveAuthorDefaultScore(groupedAuthors.map(x => x._1))
    val scoreAfterSources = Scoring.reduceScoreByAmountOfLinks(defaultScore, amountOfSourcesPerAuthor)
    val scoreAfterAmountOfArticles = Scoring.reduceByAmountOfArticles(scoreAfterSources, amountOfArticles, spark)


    // joining Dataframes
    val joinedArticles = joinDataFrames(articles, averageWords)
    val joinedPublishedWebsite = joinDataFrames(daysPublished, perWebsiteDF)
    val joinedPublishedDepartment = joinDataFrames(joinedPublishedWebsite, perDepartmentDF)
    val joinedSourcePublished = joinDataFrames(joinedPublishedDepartment, amountSourceDF)
    val joinedScorePublished = joinDataFrames(scoreAfterAmountOfArticles, joinedSourcePublished)
    val joinedLastTexts = joinDataFrames(joinedScorePublished,lastTextsDF)
    val fullDataFrame = joinDataFrames(joinedArticles, joinedLastTexts)
    // save to MongoDB
    DBConnector.writeToDB(fullDataFrame, writeConfig = writeConfig)


  }
}

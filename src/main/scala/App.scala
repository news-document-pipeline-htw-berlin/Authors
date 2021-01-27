import authorMapping.{Authors, Scoring}
import db.DBConnector
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
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


    val articlesInputFile = getConnectionInfoFromFile("src/main/resources/inputDBSettings")
    val articlesOutputFile = getConnectionInfoFromFile("src/main/resources/outputDBSettings")

    val inputUri = DBConnector.createUri(articlesInputFile.getOrElse("inputUri", throw new IllegalArgumentException),
      articlesInputFile.getOrElse("inputDB", throw new IllegalArgumentException),
      articlesInputFile.getOrElse("inputCollection", throw new IllegalArgumentException))

    val outputUri = DBConnector.createUri(articlesOutputFile.getOrElse("outputUri", throw new IllegalArgumentException),
      articlesOutputFile.getOrElse("outputDB", throw new IllegalArgumentException),
      articlesOutputFile.getOrElse("outputCollection", throw new IllegalArgumentException))

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Author analysis")
      .config("spark.mongodb.input.uri", inputUri)
      .config("spark.mongodb.output.uri", outputUri)
      .getOrCreate()

    val readConfig = DBConnector.createReadConfig(inputUri)
    val writeConfig = DBConnector.createWriteConfig(outputUri)

    val mongoData = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfig)

    // Mapping elements
    val groupedAuthors = Authors.groupByAuthor(mongoData)
    val amountOfSourcesPerAuthor = Authors.averageSourcesPerAuthor(groupedAuthors)
    val publishedOnDay = Authors.totalArticlesPerDay(groupedAuthors)
    val perWebsite = Authors.totalArticlesPerWebsite(groupedAuthors)
    val averageWordsPerArticle = Authors.averageWordsPerArticleRDD(groupedAuthors)
    val amountOfArticles = Authors.amountOfArticlesPerAuthor(groupedAuthors)
    val perDepartment = Authors.totalArticlesPerDepartment(groupedAuthors)
    val lastTexts = Authors.lastNTexts(groupedAuthors, 5)
    val sentimentPerCategory = Authors.avgSentimentPerDepartment(groupedAuthors)
    val sentimentPerDay = Authors.avgSentimentPerDay(groupedAuthors)

    // Creation of Dataframes
    val articles = spark.createDataFrame(amountOfArticles.collect()).toDF("_id", "articles")
    val averageWords = spark.createDataFrame(averageWordsPerArticle.collect()).toDF("_id", "averageWords")
    val daysPublished = spark.createDataFrame(publishedOnDay.collect()).toDF("_id", "daysPublished")
    val perWebsiteDF = spark.createDataFrame(perWebsite.collect()).toDF("_id", "perWebsite")
    val perDepartmentDF = spark.createDataFrame(perDepartment.collect()).toDF("_id", "perDepartment")
    val amountSourceDF = spark.createDataFrame(amountOfSourcesPerAuthor.collect()).toDF("_id", "avgAmountOfSources")
    val lastTextsDF = spark.createDataFrame(lastTexts).toDF("_id", "lastTexts")
    val sentimentPerCategoryDF = spark.createDataFrame(sentimentPerCategory).toDF("_id", "sentimentPerDepartment")
    val sentimentPerDayDF = spark.createDataFrame(sentimentPerDay).toDF("_id", "sentimentPerDay")

    // Trust score for authors
    val defaultScore = Scoring.giveAuthorDefaultScore(groupedAuthors.map(x => x._1))
    val scoreAfterSources = Scoring.reduceScoreByAmountOfLinks(defaultScore, amountOfSourcesPerAuthor)
    val scoreAfterAmountOfArticles = Scoring.reduceByAmountOfArticles(scoreAfterSources, amountOfArticles, spark)


    val joinedArticles = joinDataFrames(articles, averageWords)
    val joinedPublishedWebsite = joinDataFrames(daysPublished, perWebsiteDF)
    val joinedPublishedDepartment = joinDataFrames(joinedPublishedWebsite, perDepartmentDF)
    val joinedSourcePublished = joinDataFrames(joinedPublishedDepartment, amountSourceDF)
    val joinedScorePublished = joinDataFrames(scoreAfterAmountOfArticles, joinedSourcePublished)
    val joinedLastTexts = joinDataFrames(joinedScorePublished, lastTextsDF)
    val joinedSentimentDepartment = joinDataFrames(joinedLastTexts, sentimentPerCategoryDF)
    val joinedSentimentPerDay = joinDataFrames(joinedSentimentDepartment, sentimentPerDayDF)
    val fullDataFrame = joinDataFrames(joinedArticles, joinedSentimentPerDay)

    // save to MongoDB
    DBConnector.writeToDB(fullDataFrame, writeConfig = writeConfig)

  }
}

import authorMapping.{Authors, Scoring}
import db.DBConnector
import org.apache.spark.sql.{DataFrame, SparkSession}


object App {

  /**
   * Joins two SQL Dataframes by column _id
   *
   * @param frame1 First Dataframe with _id
   * @param frame2 Second DataFramce with _id
   * @return A new DataFrame joined by _id from frame1 and frame2
   */

  def joinDataFrames(frame1: DataFrame, frame2: DataFrame): DataFrame = {
    frame1.join(frame2, Seq("_id"))
  }

  def main(args: Array[String]): Unit = {

    // At first we have to specify where Spark can find both the MongoDB Collection it shall read from and then write to

    //val inputUri = DBConnector.createUri("127.0.0.1", "articles", "articles_analytics", "27017")
    val inputUri = DBConnector.createUri("127.0.0.1", "test", "nlp", "27017")
    val outputUri = DBConnector.createUri("127.0.0.1", "articles", "authors", "27017")

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Author analysis")
      .config("spark.mongodb.input.uri", inputUri)
      .config("spark.mongodb.output.uri", outputUri)
      .getOrCreate()

    // We can then create a Read- and WriteConfig to specify how Spark should read and write to the Collection specified in the URI

    val readConfig = DBConnector.createReadConfig(inputUri)
    val writeConfig = DBConnector.createWriteConfig(outputUri)

    val mongoData = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfig)

    // Then the data we need is accumulated by the independent methods, they are all independent of each other
    // so the methods can be commented out if they're not needed anymore

    val groupedAuthors = Authors.elucidateData(mongoData)
    val amountOfSourcesPerAuthor = Authors.averageSourcesPerAuthor(groupedAuthors)
    val publishedOnDay = Authors.totalArticlesPerDay(groupedAuthors)
    val perWebsite = Authors.totalArticlesPerWebsite(groupedAuthors)
    val averageWordsPerArticle = Authors.averageWordsPerArticle(groupedAuthors)
    val amountOfArticles = Authors.amountOfArticlesPerAuthor(groupedAuthors)
    val perDepartment = Authors.totalArticlesPerDepartment(groupedAuthors)
    val lastTexts = Authors.lastNTexts(groupedAuthors, 5)
    val sentimentPerCategory = Authors.avgSentimentPerDepartment(groupedAuthors)
    val sentimentPerDay = Authors.avgSentimentPerDay(groupedAuthors)

    // Because everything in the prior methods is done with RDDs, and SparkMongo can only save DataFrames to collection,
    // all RDDs have to be formed into DataFrames

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
    val scoreAfterAmountOfArticles = spark.createDataFrame(Scoring.reduceByAmountOfArticles(scoreAfterSources, amountOfArticles).collect).toDF("_id", "score")


    // Because everything is calculated independent of each other, it has to be formed to a single DataFrame
    // This has obviously a lot of refactoring potential

    val joinedArticles = joinDataFrames(articles, averageWords)
    val joinedPublishedWebsite = joinDataFrames(daysPublished, perWebsiteDF)
    val joinedPublishedDepartment = joinDataFrames(joinedPublishedWebsite, perDepartmentDF)
    val joinedSourcePublished = joinDataFrames(joinedPublishedDepartment, amountSourceDF)
    val joinedScorePublished = joinDataFrames(scoreAfterAmountOfArticles, joinedSourcePublished)
    val joinedLastTexts = joinDataFrames(joinedScorePublished, lastTextsDF)
    val joinedSentimentDepartment = joinDataFrames(joinedLastTexts, sentimentPerCategoryDF)
    val joinedSentimentPerDay = joinDataFrames(joinedSentimentDepartment, sentimentPerDayDF)

    val fullDataFrame = joinDataFrames(joinedArticles, joinedSentimentPerDay)

    // finally saves the completely joined DataFrame to the database specified in the writeConfig

    DBConnector.writeToDB(fullDataFrame, writeConfig = writeConfig)

  }
}

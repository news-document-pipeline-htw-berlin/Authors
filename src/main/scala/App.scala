import authorMapping.Authors
import db.DBConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

object App {

  def joinDataFrames(frame1: DataFrame, frame2: DataFrame): DataFrame = {
    frame1.join(frame2, Seq("_id"))
  }


  def main(args: Array[String]): Unit = {

    val inputUri = DBConnector.createUri("127.0.0.1", "artikel", "artikelcollection")
    val outputUri = DBConnector.createUri("127.0.0.1", "test", "authors")

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
    val publishedOnDay = Authors.publishedOnDayRDD(groupedAuthors)
    val perWebsite = Authors.amountOfArticlesByWebsiteRDD(groupedAuthors)
    val averageWordsPerArticle = Authors.averageWordsPerArticleRDD(groupedAuthors)
    val amountOfArticles = Authors.amountOfArticlesPerAuthor(groupedAuthors)
    val perDepartment = Authors.articlesPerDepartment(mongoData)

    // Creation of Dataframes
    val articles = spark.createDataFrame(amountOfArticles.collect()).toDF("_id", "articles")
    val averageWords = spark.createDataFrame(averageWordsPerArticle.collect()).toDF("_id", "averageWords")
    val daysPublished = spark.createDataFrame(publishedOnDay.collect()).toDF("_id", "daysPublished")
    val perWebsiteDF = spark.createDataFrame(perWebsite.collect()).toDF("_id", "perWebsite")
    val perDepartmentDF = spark.createDataFrame(perDepartment.collect()).toDF("_id", "perDepartment")

    // joining Dataframes
    val joinedArticles = joinDataFrames(articles, averageWords)
    val joinedPublishedWebsite = joinDataFrames(daysPublished, perWebsiteDF)
    val joinedPublishedDepartment = joinDataFrames(joinedPublishedWebsite, perDepartmentDF)
    val fullDataFrame = joinDataFrames(joinedArticles, joinedPublishedDepartment)

    // save to MongoDB
    DBConnector.writeToDB(fullDataFrame, writeConfig = writeConfig)


  }

}

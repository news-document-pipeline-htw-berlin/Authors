package authorMapping

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object Scoring {


  def giveAuthorDefaultScore(data: RDD[String]): RDD[(String, Double)] = {
    data.map((_, 100.0))
  }


  def reduceScoreByAmountOfLinks(scores: RDD[(String, Double)], amountOfLinks: RDD[(String, Double)]): RDD[(String, Double)] = {
    val joinedScores = scores.distinct().join(amountOfLinks)

    /*
    x._1 = Name of author
    x._2._1 = current score
    x._2._2 = average amount of sources
     */

    joinedScores.map(x => (x._1,
      if (x._2._2 < 1) x._2._1 - 30
      else if (x._2._2 < 2) x._2._1 - 15
      else x._2._1))

  }

  def reduceByAmountOfArticles(scores: RDD[(String, Double)], amountOfArticles: RDD[(String, Int)], sparkSession: SparkSession): DataFrame = {
    val maxArticles = amountOfArticles.map(_._2).max()

    val temp = amountOfArticles.map(x => x._2).filter(_ > 10).filter(_ < maxArticles).cache()
    val averageArticles = temp.sum() / temp.count()

    val joinedScores = scores.distinct().join(amountOfArticles)

    /*
    x._1 = Name of author
    x._2._1 = current score
    x._2._2 = amount of articles
     */
    val processedScore = joinedScores.map(x => (x._1,
      if (x._2._2 < averageArticles) {
        if (x._2._1 - 10 < 0) 0.0 else x._2._1 - 10
      }
      else if (x._2._2 > averageArticles) {
        if (x._2._1 + 10 > 100) 100.0 else x._2._1 + 10
      }
      else x._2._1))

    sparkSession.createDataFrame(processedScore.collect()).toDF("_id", "score")

  }


}

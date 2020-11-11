package authorMapping

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object Scoring {


  def giveAuthorDefaultScore(data: RDD[String], sparkSession: SparkSession): DataFrame = {
    val defaultScore = data.map((_, 100.0))
    sparkSession.createDataFrame(defaultScore.collect()).toDF("_id", "score")
  }


  def reduceScoreByAmountOfLinks(scoreDataFrame: DataFrame, amountLinksDataFrame: DataFrame, sparkSession: SparkSession): DataFrame = {
    val data = amountLinksDataFrame.join(scoreDataFrame, Seq("_id")).rdd

    val processedScore = data.map(x => (x(0).toString,
      if (x.getDouble(1) < 1) x.getDouble(2) - 30
      else if (x.getDouble(1) < 2) x.getDouble(2) - 15
      else x.getDouble(2)))

    sparkSession.createDataFrame(processedScore.collect()).toDF("_id", "score")

  }

  def reduceByAmountOfArticles(scoreDataFrame: DataFrame, amountOfArticles: RDD[(String, Double)], sparkSession: SparkSession): DataFrame = {
    val maxArticles = amountOfArticles.sortBy(_._2, ascending = false).map(_._2).take(1).head
    val averageArticles = amountOfArticles.filter(_._2 > 10).filter(_._2 < maxArticles).map(x => List(x._2)).map(y => y.sum / y.length).collect().head

    val avgArticles = sparkSession.createDataFrame(amountOfArticles.collect()).toDF("_id", "avgArticles")
    val data = avgArticles.join(scoreDataFrame, Seq("_id")).rdd
    val processedScore = data.map(x => (x(0).toString,
      if (x.getDouble(1) < averageArticles){ if(x.getDouble(2) - 10 < 0) 0.0 else x.getDouble(2) - 10  }
      else if (x.getDouble(1) > averageArticles){ if(x.getDouble(2) + 10 > 100) 100.0 else x.getDouble(2) + 10 }
      else x.getDouble(2)))

    sparkSession.createDataFrame(processedScore.collect()).toDF("_id", "score")

  }


}

package authorMapping

import org.apache.spark.rdd.RDD


/**
 * An object which aims to give each author a trust score given their overall credibility
 * It only works after some data has already been extracted from the overall data, namely the averageAmountOfSources
 * and the amountOfArticlesPerAuthor
 */
object Scoring {


  /**
   * Maps the value 100 to every author given in the data: RDD.
   * The value 100 is seen as trusted, meaning every author is seen as completely trusted without further evidence
   *
   * @param data an RDD consisting of the names of all authors which shall receive a score
   * @return an RDD consisting of Name of the author and 100 (default score)
   */
  def giveAuthorDefaultScore(data: RDD[String]): RDD[(String, Double)] = {
    data.map((_, 100.0))
  }


  /**
   * The current score is lowered according to the amount of sources the author uses
   * If the author uses less than one source on average over all his/her articles the score is lowered by 30,
   * if the average is less than two, the score is lowered by 15.
   * This method of scoring can use a lot of fine tuning and either better specification for authors or a relative value extracted from the data
   *
   * @param scores        an RDD which consists of Author and current Score, doesn't need to be from "giveAuthorDefaultScore"
   * @param amountOfLinks the RDD "Authors.averageSourcesPerAuthor" creates
   * @return a new RDD with authors and their potentially new scores
   */
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

  /**
   * First off the average amount of articles over all authors is created
   * Then all authors are examined and their score is lowered if they've written less than the overall average
   * or is  increased if they've written more than the average
   * The author with the most articles is filtered  from the dataset, because in the data it was significantly higher than the rest
   *
   * @param scores           an RDD which consists of Author and current Score, doesn't need to be from "giveAuthorDefaultScore"
   * @param amountOfArticles the RDD "Authors.amountOfArticlesPerAuthor" creates
   * @return a new RDD with authors and their potentially new scores
   */
  def reduceByAmountOfArticles(scores: RDD[(String, Double)], amountOfArticles: RDD[(String, Int)]): RDD[(String, Double)] = {
    val maxAmountOfArticles = amountOfArticles.map(_._2).max()

    val filteredAmountOfArticles = amountOfArticles.map(x => x._2).filter(_ > 10).filter(_ < maxAmountOfArticles).cache()
    val averageArticlesOverAllAuthors = filteredAmountOfArticles.sum() / filteredAmountOfArticles.count()

    val joinedScores = scores.distinct().join(amountOfArticles)

    /*
    x._1 = Name of author
    x._2._1 = current score
    x._2._2 = amount of articles
     */
    joinedScores.map(x => (x._1,
      if (x._2._2 < averageArticlesOverAllAuthors) {
        if (x._2._1 - 10 < 0) 0.0 else x._2._1 - 10
      }
      else if (x._2._2 > averageArticlesOverAllAuthors) {
        if (x._2._1 + 10 > 100) 100.0 else x._2._1 + 10
      }
      else x._2._1))


  }


}

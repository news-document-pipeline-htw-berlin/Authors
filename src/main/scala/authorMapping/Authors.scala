package authorMapping

import java.time.format.TextStyle
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util.Locale
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.AnalysisException
import scala.collection.mutable

/**
 * An object which maps data of interest to each author separately
 */
object Authors {


  /**
   * This method elucidates and extracts the necessary data from the implicit DataFrame from the MongoDB collection you read prior to this.
   * At first it tries to extract the columns specified in "data.select", if one or all are missing an AnalysisException is thrown and the algorithm stops.
   * Then it simply maps the name of the author to: the text he has written, the time it was published or the time it was crawled if published_time isn't available,
   * the website the article was published in, a List of links for the article, the department (category) of the article and the sentiment value of the article which was determined
   * in the NLP Pipeline.
   *
   * @param data the DataFrame from reading the NLP Database
   * @return an RDD consisting of: Author, Text, PublishedTime, NewsSite, Sources, Departments and Sentiments
   * @throws AnalysisException gets thrown if none or not all columns of data.select are present, gets thrown because
   *                           all data specified is needed and the program will otherwise not work.
   *                           When this exception gets thrown the system will stop and Spark will close.
   */
  def elucidateData(data: DataFrame): RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))] = {
    try {
      val temp = data.select("authors", "text", "published_time", "crawl_time", "news_site", "links", "department", "sentimens").rdd
      temp.flatMap(x => x.getAs[mutable.WrappedArray[String]](0).toList.map(y => (y, (x.getString(1), if (x.get(2) != null) x.getTimestamp(2) else x.getTimestamp(3), x.getString(4),
        x.getAs[mutable.WrappedArray[String]](5).toList, x.getAs[mutable.WrappedArray[String]](6).toList, x.getDouble(7)))))

    } catch {
      case ex: AnalysisException =>
        println("Wrong schema")
        sys.exit(-100)
    }

  }


  /**
   * This method first calculates the sum of the amount of articles per author.
   * reduceByKey is used, because it can be faster than groupByKey due to Spark's partitioning
   * Saved as an Integer in MongoDB Collection
   *
   * @param data the elucidated data from elucidateData
   * @return an RDD which consists of tuples, which furthermore consist of the name of the author and  the amount of articles that were counted in the data
   *         Example : RDD[(Anna Krueger,20),(Hendrik, 5)]
   */
  def amountOfArticlesPerAuthor(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Int)] = {
    data.map(x => (x._1, 1)).reduceByKey(_ + _)
  }

  /**
   * This method tokenizes the text at first with the given regex, then splits them to an Array and maps the length to a 1.0
   * which is used for counting the amount of articles for the author
   * It is then reduced by the key (name of the author) and both the counter and the word count are summed up
   * At last the average is built by dividing the sum of the word count per author by the sum of the counter (amount of articles per author)
   *
   * Saved as a Double in MongoDB Collection
   *
   * @param data the elucidated data from elucidateData
   * @return an RDD which consists of tuples, which furthermore consist of the name of the author and the average amount of words over all articles written by them
   *         Example : RDD[(Anna Krueger,306.66)]
   */
  def averageWordsPerArticle(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Double)] = {
    data.mapValues(x => (x._1.replace(",", "").split(" ").length.toDouble, 1.0)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2))
  }


  /**
   * It works similar to "averageWordsPerArticle", but this time we don't have to count the amount of words
   * We simply take the length of the Array of sources, map a counter to it, reduce it by the key (author)
   * and finally divide the sum of the links per author by the sum of the counter per author (amount of articles per author)
   *
   * Saved as a Double in MongoDB Collection
   *
   * @param data the elucidated data from elucidateData
   * @return an RDD which consists of tuples, which furthermore consist of the name of the author and the average amount of sources over all articles written by them
   *         Example : RDD[(Anna Krueger,1.61)]
   */

  def averageSourcesPerAuthor(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Double)] = {
    data.mapValues(x => (x._4.size, 1.0)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => y._1 / y._2)
  }


  /**
   * Maps every author to an Array (Day, Amount) => (Day the article was published, amount of times an article was published on this day by this author)
   * Days are ordered from Monday to Sunday
   * The second element of the tuple is an Array, because, although you can order a Map in Scala, MongoDB doesn't care about the ordering
   * However with an Array the order is preserved
   *
   * Saved as a List in MongoDB Collection
   *
   * @param data the elucidated data from elucidateData
   * @return an RDD which consists of tuples, which furthermore consist of the name of the author and an Array of Tuples with the name of the weekday and the
   *         amount of articles that were published for the author on this day
   *
   *         Example: RDD[(Anna Krueger -> Array((Thursday,2), (Friday, 5)))]
   */

  def totalArticlesPerDay(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Array[(String, Int)])] = {
    val x = data.mapValues(x => x._2).groupByKey().map(x => (x._1, x._2.toList.map(x => OffsetDateTime.ofInstant(Instant.ofEpochMilli(x.getTime), ZoneId.of("Z")).getDayOfWeek)))
    x.map(x => (x._1, x._2.map(y => (y.getValue, y, x._2.count(_ == y))).sortBy(x => x._1).map(x => (x._2.getDisplayName(TextStyle.FULL, Locale.ENGLISH), x._3)).distinct.toArray))
  }


  /**
   *
   * Maps every author to an Array (Department, Amount) => (Department of the article, amount of times an article was published in this department by this author)
   *
   * Saved as a List in MongoDB Collection
   *
   * @param data the elucidated data from elucidateData
   * @return an RDD which consists of tuples, which furthermore consist of the name of the author and an Array of Tuples with the department and the
   *         amount of articles that were published for the author in this department
   *
   *         Examplpe: RDD[(Anna Krueger, Array((Wissen,2), (Reisen, 5)))]
   */

  def totalArticlesPerDepartment(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Array[(String, Int)])] = {
    data.mapValues(_._5).groupByKey().map(x => (x._1, x._2.flatten.map(y => (y, x._2.flatten.count(_ == y))).toArray.distinct))
  }


  /**
   * The same as articlesPerDepartment, just that it maps the average sentiment per department and not the amount per Department
   *
   * Saved as a List in MongoDB Collection
   *
   * @param data the elucidated data from elucidateData
   * @return an RDD which consists of tuples, which furthermore consist of the name of the author and an Array of Tuples with the department and the
   *         average sentiment value of the articles that were published for the author in this department
   *
   *         Example: RDD[(Anna Krueger, Array((Wissen,3.16), (Reisen, -1.1)))]
   */

  def avgSentimentPerDepartment(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Array[(String, Double)])] = {
    data.mapValues(x => (x._5, x._6)).groupByKey().mapValues(y => y.flatMap(z => z._1.map(f => (f, z._2))).groupBy(_._1).map(x => (x._1, x._2.map(z => z._2).sum / x._2.size)).toArray)

  }


  /**
   * The same as totalArticlesPerDay, just that it maps the average sentiment per Day and not the amount of articles per Day
   *
   * Saved as a List in MongoDB Collection
   *
   * @param data the elucidated data from elucidateData
   * @return an RDD which consists of tuples, which furthermore consist of the name of the author and an Array of Tuples with the name of the weekday and the
   *         average sentiment value of the articles that were published for the author on this weekday
   *
   *         Example: RDD[(Anna Krueger, Array((Thursday,3.16), (Friday, -1.1)))]
   */

  def avgSentimentPerDay(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Array[(String, Double)])] = {
    val x = data.mapValues(x => (x._2, x._6)).groupByKey().mapValues(x => x.map(y => (OffsetDateTime.ofInstant(Instant.ofEpochMilli(y._1.getTime), ZoneId.of("Z")).getDayOfWeek, y._2)))
    x.mapValues(z => z.groupBy(_._1).map(y => (y._1.getValue, y._1.getDisplayName(TextStyle.FULL, Locale.ENGLISH), y._2.map(f => f._2).map(z => z).sum / y._2.size)).toArray.sortBy(_._1).map(x => (x._2, x._3)).distinct)

  }


  /**
   * Maps the name of the author to a Map consisting of the short name of the website (postillion, golem etc.) to the amount of articles
   * that were found on this site, by this author
   *
   * Saved as a Map in MongoDB Collection
   *
   * @param data the elucidated data from elucidateData
   * @return an RDD which consists of tuples, which furthermore consists of the name of the author and a Map  with the name of the Website as key and the
   *         amount of articles that were published for the author on this website
   *
   *         Example: Map(Anna Krueger -> Map(golem -> 22, postillion -> 3))
   */

  def totalArticlesPerWebsite(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Map[String, Double])] = {
    data.mapValues(x => x._3).groupByKey().map(x => (x._1, x._2.toList.map(y => (y, x._2.count(_ == y).toDouble)).toMap))
  }


  /**
   * Maps the amount of word of the last N texts written by an author
   * The amount of texts can be specified by amount and was set to five after a meeting
   * However you can specify as much as you'd like
   * The Array entries are sorted by the date in ascending order:  (2020-10-12, 2020-11-24, 2021-01-14)
   * If the author has less than N articles, only as many as he has are taken.
   * Meaning if you specify N=5 and the author only  has two articles, only two are taken without an exception.
   *
   * Saved as a List in MongoDB Collection
   *
   * @param data   the elucidated data from elucidateData
   * @param amount the amount of articles you want the word count for
   * @return an RDD which consists of tuples, which furthermore consists of the name of the author and an Array with the date in UTC format (YYYY-MM-DD) and the
   *         word count of the given article
   *
   *         Example: RDD[(("Anna Krueger", Array(("2020-12-03", 11), ("2020-12-05", 10)))]
   */

  def lastNTexts(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))], amount: Int): RDD[(String, Array[(String, Int)])] = {
    data.mapValues(x => (x._1.replace(",", "").split(" ").length, OffsetDateTime.ofInstant(Instant.ofEpochMilli(x._2.getTime), ZoneId.of("Z"))))
      .mapValues(_.swap).groupByKey().mapValues(x => x.toList.sortBy(_._1).takeRight(amount).map(x => (x._1.toLocalDate.toString, x._2)).toArray)
  }

}

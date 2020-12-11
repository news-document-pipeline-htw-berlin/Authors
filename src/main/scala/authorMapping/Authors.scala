package authorMapping

import java.time.format.TextStyle
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable

object Authors {

  val split_regex = "\\W+"


  /*
   Aggregates every entry to a Map with (key,value) => (Author, (Text,Date,Website,NewsSite,Links,Departments))
   If no published date is provided, crawl time will be used instead
  */

  def groupByAuthorRDDRow(data: RDD[Row]): RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))] = {
    data.flatMap(x => x.getAs[mutable.WrappedArray[String]](1).toList.map(y => (y, (x.getString(15), if (x.get(13) != null) x.getTimestamp(13) else x.getTimestamp(2), x.getString(12),
      x.getAs[mutable.WrappedArray[String]](9).toList, x.getAs[mutable.WrappedArray[String]](3).toList, x.getString(17).toDouble))))
  }


  /*
   Maps every author to the amount of articles they've written (which are in db)
   e.g. (Anna Krueger, 20)
  */

  def amountOfArticlesPerAuthor(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Int)] = {
    data.map(x => (x._1, 1)).reduceByKey(_ + _)
  }

  /*
   Maps every author to his average amount of words per article
   e.g Map (Anna Krueger -> 306.66)
  */

  def averageWordsPerArticleRDD(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Double)] = {
    data.mapValues(x => (x._1.replace(",", "").split(" ").length.toDouble, 1.0)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2))
  }

  /*
   Maps every author to its average amount of sources used over all articles
   e.g Map[Anna Krueger -> 1.61]
  */

  def averageSourcesPerAuthor(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Double)] = {
    data.mapValues(x => (x._4.size, 1.0)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => y._1 / y._2)
  }


  /*
  Maps every author to an Array (Day, Amount) => (Day the article was published, amount of times an article was published on this day)
  e.g. RDD[(Anna Krueger -> Array((Thursday,2), (Friday, 5)))]
  Days are ordered from Monday to Sunday
 */

  def amountOfArticlesPerDay(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Array[(String, Int)])] = {
    val x = data.mapValues(x => x._2).groupByKey().map(x => (x._1, x._2.toList.map(x => OffsetDateTime.ofInstant(Instant.ofEpochMilli(x.getTime), ZoneId.of("Z")).getDayOfWeek)))
    x.map(x => (x._1, x._2.map(y => (y.getValue, y, x._2.count(_ == y))).sortBy(x => x._1).map(x => (x._2.getDisplayName(TextStyle.FULL, Locale.ENGLISH), x._3)).distinct.toArray))
  }

  /*
   Maps every Author to another Map with (key,value) => (Name of category, amount of times an article was published in this category)
   e.g. RDD[(Anna Krueger, Array((Wissen,2), (Reisen, 5)))]
  */

  def amountOfArticlesPerDepartment(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Array[(String, Int)])] = {
    data.mapValues(_._5).groupByKey().map(x => (x._1, x._2.flatten.map(y => (y, x._2.flatten.count(_ == y))).toArray.distinct))
  }

  /*
    The same as articlesPerDeparment, just that it maps the average sentiment per department and not the amount per Department
     e.g. RDD[(Anna Krueger, Array((Wissen,3.16), (Reisen, -1.1)))]
   */

  def avgSentimentPerDepartment(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Array[(String, Double)])] = {
    data.mapValues(x => (x._5, x._6)).groupByKey().mapValues(y => y.flatMap(z => z._1.map(f => (f, z._2))).groupBy(_._1).map(x => (x._1, x._2.map(z => z._2).sum / x._2.size)).toArray)

  }

  /*
    The same als publishedOnDay, just that it maps the average sentiment per Day and not the amount of articles per Day
    e.g. RDD[(Anna Krueger, Array((Thursday,-1.13), (Friday, 0.25)))]
   */

  def avgSentimentPerDay(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Array[(String, Double)])] = {
    val x = data.mapValues(x => (x._2, x._6)).groupByKey().mapValues(x => (x.map(y => (OffsetDateTime.ofInstant(Instant.ofEpochMilli(y._1.getTime), ZoneId.of("Z")).getDayOfWeek, y._2))))
    x.mapValues(z => z.groupBy(_._1).map(y => (y._1.getValue, y._1.getDisplayName(TextStyle.FULL, Locale.ENGLISH), y._2.map(f => f._2).map(z => z).sum / y._2.size)).toArray.sortBy(_._1).map(x => (x._2, x._3)).distinct)

  }


  /*
   Maps the author to another map with (key,value) => (news_page, amount of times published on news_page)
   e.g. Map(Anna Krueger -> Map(sz -> 22, taz -> 3))
  */

  def amountOfArticlesByWebsiteRDD(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))]): RDD[(String, Map[String, Double])] = {
    data.mapValues(x => x._3).groupByKey().map(x => (x._1, x._2.toList.map(y => (y, x._2.count(_ == y).toDouble)).toMap))
  }

  /*
    Maps the last N texts written by an author, sorted by date
    e.g. Array(("Anna Krueger", Array(("2020-12-03", 11), ("2020-12-05", 10)))
   */

  def lastNTexts(data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))], amount: Int): RDD[(String, Array[(String, Int)])] = {
    data.mapValues(x => (x._1.replace(",", "").split(" ").length, OffsetDateTime.ofInstant(Instant.ofEpochMilli(x._2.getTime), ZoneId.of("Z"))))
      .mapValues(_.swap).groupByKey().mapValues(x => x.toList.sortBy(_._1).takeRight(amount).map(x => (x._1.toLocalDate.toString, x._2)).toArray)
  }

}

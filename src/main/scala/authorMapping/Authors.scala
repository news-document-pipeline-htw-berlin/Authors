package authorMapping

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable

object Authors {

  /*
   Aggregates every entry to a Map with (key,value) => (Author, (Text,Date,Website,NewsSite,Links,Departments))
   If no published date is provided, crawl time will be used instead
  */

  def groupByAuthorRDDRow(data: RDD[Row]): RDD[(String, (String, Any, String, List[String], List[String]))] = {
    data.flatMap(x => x.getAs[mutable.WrappedArray[String]](1).toList.map(y => (y, (x.getString(15), if (x.get(13) != null) x.get(13) else x.get(2), x.getString(12),
      x.getAs[mutable.WrappedArray[String]](9).toList, x.getAs[mutable.WrappedArray[String]](3).toList))))
  }


  /*
   Maps every author to the amount of articles they've written (which are in db)
   e.g. (Anna Krueger, 20)
  */

  def amountOfArticlesPerAuthor(data: RDD[(String, (String, Any, String, List[String], List[String]))]): RDD[(String, Int)] = {
    data.map(x => (x._1, 1)).reduceByKey(_ + _)
  }

  /*
   Maps every author to his average amount of words per article
   e.g Map (Anna Krueger -> 306.66)
  */

  def averageWordsPerArticleRDD(data: RDD[(String, (String, Any, String, List[String], List[String]))]): RDD[(String, Double)] = {
    data.mapValues(x => (x._1.replace(",", "").split(" ").length.toDouble, 1.0)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2))
  }

  /*
   Maps every Authors to its average amount of sources used over all articles
   e.g Map[Anna Krueger -> 1.61]
  */

  def averageSourcesPerAuthor(data: RDD[(String, (String, Any, String, List[String], List[String]))]): RDD[(String, Double)] = {
    data.mapValues(x => (x._4.size, 1.0)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => y._1 / y._2)
  }


  /*
  Maps every Author to another Map with (key,value) => (Day the article was published, amount of times an article was published on this day)
  e.g. Map[Anna Krueger -> Map(Thu -> 2, Fri -> 5)]
 */

  def publishedOnDayRDD(data: RDD[(String, (String, Any, String, List[String], List[String]))]): RDD[(String, Map[String, Int])] = {
    val formatter = new SimpleDateFormat("EEEE", Locale.ENGLISH)
    val parser = new SimpleDateFormat("yyyy-MM-dd")

    val x = data.mapValues(x => x._2).groupByKey().map(x => (x._1, x._2.toList.map(x => if (x != null) formatter.format(parser.parse(x.toString)) else "null")))
    x.map(x => (x._1, x._2.map(y => (y, x._2.count(_ == y))).toMap))
  }

  /*
   Maps every Author to another Map with (key,value) => (Name of category, amount of times an article was published in this category)
   e.g. Map[Anna Krueger -> Map(Wissen -> 25, Reisen -> 2)]
  */

  def articlesPerDepartment(data: RDD[(String, (String, Any, String, List[String], List[String]))]): RDD[(String, Map[String, Int])] = {
    data.mapValues(_._5).groupByKey().map(x => (x._1, x._2.flatten.map(y => (y, x._2.flatten.count(_ == y))).toMap))
  }


  /*
   Maps the author to another map with (key,value) => (news_page, amount of times published on news_page)
   e.g. Map(Anna Krueger -> Map(sz -> 22, taz -> 3))
  */

  def amountOfArticlesByWebsiteRDD(data: RDD[(String, (String, Any, String, List[String], List[String]))]): RDD[(String, Map[String, Double])] = {
    data.mapValues(x => x._3).groupByKey().map(x => (x._1, x._2.toList.map(y => (y, x._2.count(_ == y).toDouble)).toMap))
  }
}

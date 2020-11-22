package authorMapping

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable

object Authors {

  /*
     Aggregates every entry to a Map with (key,value) => (Author, (Text,Date,Website))
     If no published date is provided, crawl time will be used instead
  */

  def groupByAuthorRDDRow(data: RDD[Row]): RDD[(String, List[(String, Any, String, String, List[String], List[String])])] = {
    data.flatMap(x => x.getAs[mutable.WrappedArray[String]](1).toList.map(y => (y, (x.getString(15), if (x.get(13) != null) x.get(13) else x.get(2), x.getString(12), x.getString(16),
      x.getAs[mutable.WrappedArray[String]](9).toList, x.getAs[mutable.WrappedArray[String]](3).toList)))).groupByKey().map(x => (x._1, x._2.toList))
  }


  /*
     Maps every author to the amount of articles they've written (which are in db)
     e.g. (Anna Krueger, 20)
  */

  def amountOfArticlesPerAuthor(data: RDD[(String, List[(String, Any, String, String, List[String], List[String])])]): RDD[(String, Double)] = {
    data.map(x => (x._1, x._2.size))

  }

  /*
     Maps every author to his average amount of words per article
     e.g Map (Anna Krueger -> 306.66)
  */

  def averageWordsPerArticleRDD(data: RDD[(String, List[(String, Any, String, String, List[String], List[String])])]): RDD[(String, Double)] = {
    data.map(x => (x._1, x._2.map(y => y._1.replace(",", "").split(" ").length.toDouble / x._2.size).sum))

  }

  /*
    Maps the author to another map with (key,value) => (news_page, amount of times published on news_page)
    e.g. Map(Anna Krueger -> Map(sz -> 22, taz -> 3))
  */

  def amountOfArticlesByWebsiteRDD(data: RDD[(String, List[(String, Any, String, String, List[String], List[String])])]): RDD[(String, Map[String, Int])] = {
    data.map(x => (x._1, x._2.map(y => (y._3, x._2.count(_._3 == y._3))).toMap))

  }

  /*
      Maps every Author to another Map with (key,value) => (Day the article was published, amount of times an article was published on this day)
      e.g. Map[Anna Krueger -> Map(Thu -> 2, Fri -> 5)]
  */

  def publishedOnDayRDD(data: RDD[(String, List[(String, Any, String, String, List[String], List[String])])]): RDD[(String, Map[String, Int])] = {
    val formatter = new SimpleDateFormat("EEEE", Locale.ENGLISH)
    val parser = new SimpleDateFormat("yyyy-MM-dd")

    val x = data.map(x => (x._1, x._2.map(x => if (x._2 != null) formatter.format(parser.parse(x._2.toString)) else "null")))
    x.map(x => (x._1, x._2.map(y => (y, x._2.count(_ == y))).toMap))

  }

  /*
        Maps every Author to another Map with (key,value) => (Name of category, amount of times an article was published in this category)
        e.g. Map[Anna Krueger -> Map(Wissen -> 25, Reisen -> 2)]
  */

  def articlesPerDepartment(data: RDD[(String, List[(String, Any, String, String, List[String], List[String])])]): RDD[(String, Map[String, Int])] = {
    data.map(x => (x._1, x._2.map(_._6))).map(x => (x._1, x._2.flatten.map(z => (z, x._2.flatten.count(_ == z))).toMap))
  }

  /*
    Maps every Authors to its average amount of sources used over all articles
    e.g Map[Anna Krueger -> 1.61]
   */

  def authorWithArticleAndSource(data: RDD[(String, List[(String, Any, String, String, List[String], List[String])])]): RDD[(String, Double)] = {
    data.map(x => (x._1, x._2.map(y => y._5.size.toDouble / x._2.size).sum))
  }


}

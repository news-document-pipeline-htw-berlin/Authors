package authorMapping

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable

object Authors {


  def groupByAuthorRDDRow(data: RDD[Row]): RDD[(String, List[(String, Any, String)])] = {
    data.flatMap(x => x.getAs[mutable.WrappedArray[String]](1).toList.map(y => (y, (x.getString(15), x.get(13), x.getString(12))))).groupByKey().map(x => (x._1, x._2.toList))

  }

  def amountOfArticlesPerAuthor(data: RDD[(String, List[(String, Any, String)])]): RDD[(String, Double)] = {
    data.map(x => (x._1, x._2.size))

  }


  def averageWordsPerArticleRDD(data: RDD[(String, List[(String, Any, String)])]): RDD[(String, Double)] = {
    data.map(x => (x._1, x._2.map(y => y._1.split(" ").length.toDouble / x._2.size).sum))

  }

  def amountOfArticlesByWebsiteRDD(data: RDD[(String, List[(String, Any, String)])]): RDD[(String, Map[String, Int])] = {
    data.map(x => (x._1, x._2.map(y => (y._3, x._2.count(_._3 == y._3))).toMap))

  }

  def publishedOnDayRDD(data: RDD[(String, List[(String, Any, String)])]): RDD[(String, Map[String, Int])] = {
    val formatter = new SimpleDateFormat("EEEE", Locale.ENGLISH)
    val parser = new SimpleDateFormat("yyyy-MM-dd")

    val x = data.map(x => (x._1, x._2.map(x => if (x._2 != null) formatter.format(parser.parse(x._2.toString)) else "null")))
    x.map(x => (x._1, x._2.map(y => (y, x._2.count(_ == y))).toMap))

  }

}

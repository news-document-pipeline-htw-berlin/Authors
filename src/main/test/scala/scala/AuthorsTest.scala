
import authorMapping.Authors._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite


class AuthorsTest extends FunSuite {
  var data: RDD[(String, (String, java.sql.Timestamp, String, List[String], List[String], Double))] = _
  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Author tests")
    .getOrCreate()

  val testRDD = List(
    // Tuesday
    ("Steven", ("Dies ist ein Text zum testen", java.sql.Timestamp.valueOf("2020-12-08 19:45:14"), "htw", List("https://linktoPage", "http://linktoanotherpage"), List("Politik", "Gaming"), -1.2)),
    // Tuesday
    ("Steven", ("Dies ist ein positiver Test zum testen", java.sql.Timestamp.valueOf("2020-12-08 19:45:14"), "htw", List(), List("Gaming"), 0.0)),
    // Thursday
    ("Steven", ("Irgendwas über Autoren, höre da ehrlich gesagt nicht zu.", java.sql.Timestamp.valueOf("2020-11-05 12:22:52"), "htw", List(), List("Unnützes Wissen"), -2.7)),
    // Thursday
    ("Steven", ("Scheint die DB Anbindung fertig zu haben, die 40 min lange Diskussion war allerdings zu lang.", java.sql.Timestamp.valueOf("2020-11-12 12:48:10"), "htw", List(), List("Unnützes Wissen"), 0.25)),
    //Thursday
    ("Lennart", ("Ein höchstmotivierende Rede zum Start in das Gruppenmeeting", java.sql.Timestamp.valueOf("2020-12-10 12:17:30"), "htw", List("https://linktoPage"), List("Wissen"), 3.2)),
    // Thursday
    ("Lennart", ("Legt anscheinend mehr Fokus auf Content Management, sonst irgendwas mit Stopwords. ( CM Beleg ist auch ekelig )", java.sql.Timestamp.valueOf("2020-12-10 12:14:30"), "htw", List("https://linktoPage"), List("Wissen"), 1.0)),
    // Thursday
    ("Malte", ("Ein paar Worte zum aktuellen Stand, wenn das Mikrofon angeschaltet wäre", java.sql.Timestamp.valueOf("2020-12-03 12:30:00"), "htw", List("https://linktoPage", "https://soManyLinks", "http://linktoanotherpage"), List("IT"), 0.0)),
    // Thursday
    ("Malte", ("Verbindung von Backend DB für Frontend, klingt wie ein Clusterfuck", java.sql.Timestamp.valueOf("2020-12-03 12:30:00"), "htw", List("https://linktoPage", "https://soManyLinks", "http://linktoanotherpage"), List("IT"), 1.5)),
    // Monday
    ("Hendrik", ("Leider wurde die Datenbank genuked", java.sql.Timestamp.valueOf("2020-12-07 17:00:42"), "htw", List("News aus dem Labor"), List("IT"), -0.7)),
    // Wednesday
    ("Lennart", ("Dies ist ein Text zum testen. Dies ist ein, Test, zum testen, von Satzzeichen, und dem Regex.", java.sql.Timestamp.valueOf("2020-11-18 11:10:00"), "htw", List("DYOR", "Discord"), List("Politik", "Nice to know"), 1.8)))
  data = spark.sparkContext.parallelize(testRDD).cache()


  test("Amount of Articles Per Author") {
    val result = amountOfArticlesPerAuthor(data).collect().sortBy(_._2).reverse
    println(result.mkString("Array(", ", ", ")"))
    assert(result.sameElements(Array(("Steven", 4), ("Lennart", 3), ("Malte", 2), ("Hendrik", 1))))
  }

  test("Average Words per Article") {
    val result = averageWordsPerArticle(data).collect().sortBy(_._2).reverse
    println(result.mkString("Array(", ", ", ")"))
    assert(result.sameElements(Array(("Lennart", 14.333333333333334), ("Malte", 10.5), ("Steven", 9.5), ("Hendrik", 5.0))))
  }

  test("Published on Day") {
    val realResult = totalArticlesPerDay(data).collect()
    val projectedResult = Array(("Malte", Array(("Thursday", 2))), ("Hendrik", Array(("Monday", 1))), ("Lennart", Array(("Wednesday", 1), ("Thursday", 2))), ("Steven", Array(("Tuesday", 2), ("Thursday", 2))))
    assert(realResult.map(x => x._1) === projectedResult.map(x => x._1))
    assert(realResult.map(_._2) === projectedResult.map(_._2))
  }

  test("Articles per Department") {
    val realResult = totalArticlesPerDepartment(data).collect()

    val projectedResult = Array(("Malte", Array(("IT", 2))), ("Hendrik", Array(("IT", 1))), ("Lennart", Array(("Wissen", 2), ("Politik", 1), ("Nice to know", 1))), ("Steven", Array(("Politik", 1), ("Gaming", 2), ("Unnützes Wissen", 2))))

    assert(realResult.map(_._1) === projectedResult.map(_._1))
    assert(realResult.map(_._2) === projectedResult.map(_._2))
  }

  test("Sentiments per Department") {
    val realResult = avgSentimentPerDepartment(data).collect()
    val projectedResult = Array(("Malte", Array(("IT", 0.75))), ("Hendrik", Array(("IT", -0.7))), ("Lennart", Array(("Politik", 1.8), ("Nice to know", 1.8), ("Wissen", 2.1))), ("Steven", Array(("Unnützes Wissen", -1.225), ("Politik", -1.2), ("Gaming", -0.6))))

    assert(realResult.map(_._1) === projectedResult.map(_._1))
    assert(realResult.map(_._2) === projectedResult.map(_._2))
  }

  test("Sentiments per Day") {
    val realResult = avgSentimentPerDay(data).collect()
    val projectedResult = Array(("Malte", Array(("Thursday", 0.75))), ("Hendrik", Array(("Monday", -0.7))), ("Lennart", Array(("Wednesday", 1.8), ("Thursday", 2.1))), ("Steven", Array(("Tuesday", -0.6), ("Thursday", -1.225))))
    assert(realResult.map(x => x._1) === projectedResult.map(x => x._1))
    assert(realResult.map(_._2) === projectedResult.map(_._2))
  }

  test("Articles per Website") {
    val realResult = totalArticlesPerWebsite(data).collect().sortBy(_._2.values).reverse
    val projectedResult = List(("Steven", Map("htw" -> 4)), ("Lennart", Map("htw" -> 3)), ("Malte", Map("htw" -> 2)), ("Hendrik", Map("htw" -> 1)))

    assert(realResult.map(_._1) === projectedResult.map(_._1))
    assert(realResult.map(_._2) === projectedResult.map(_._2))

  }

  test("Average Sources per Author") {
    val realResult = averageSourcesPerAuthor(data).collect().sortBy(_._2).reverse
    val projectedResult = Array(("Malte", 3.0), ("Lennart", 1.3333333333333333), ("Hendrik", 1.0), ("Steven", 0.5))

    assert(realResult === projectedResult)
  }

  test("lastNTexts per Author") {
    val realResult = lastNTexts(data, 5).collect()
    val projectedResult = Array(("Malte", Array(("2020-12-03", 11), ("2020-12-03", 10))),
      ("Hendrik", Array(("2020-12-07", 5))),
      ("Lennart", Array(("2020-11-18", 17), ("2020-12-10", 18), ("2020-12-10", 8))),
      ("Steven", Array(("2020-11-05", 9), ("2020-11-12", 16), ("2020-12-08", 6), ("2020-12-08", 7))))

    assert(realResult.map(_._1) === projectedResult.map(_._1))
    assert(realResult.map(_._2) === projectedResult.map(_._2))


  }


}

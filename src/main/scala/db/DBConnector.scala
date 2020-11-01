package db

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DBConnector {


  def createUri(
                 serverAddress: String,
                 db: String,
                 collection: String): String = {
    "mongodb://" + serverAddress + "/" + db + "." + collection
  }


  def createReadConfig(inputUri: String, sparkSession: SparkSession): ReadConfig = {
    ReadConfig(Map("spark.mongodb.input.uri" -> inputUri, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkSession)))
  }

  def createWriteConfig(outputUri: String, replaceDocument: String = "false", mode: String = "overwrite", sparkSession: SparkSession): WriteConfig = {
    WriteConfig(Map("spark.mongodb.output.uri" -> outputUri,"replaceDocument" -> replaceDocument, "mode" -> mode), Some(WriteConfig(sparkSession)))

  }

  def readFromDB(sparkSession: SparkSession, readConfig: ReadConfig): RDD[Row] = {
    val rdd = MongoSpark.load(sparkSession, readConfig).rdd
    if (rdd.isEmpty())
      throw new IllegalArgumentException("Empty DB")
    else
      rdd
  }

  def writeToDB(savedInstance: DataFrame, writeConfig: WriteConfig): Unit = {
    MongoSpark.save(savedInstance, writeConfig)
  }


}

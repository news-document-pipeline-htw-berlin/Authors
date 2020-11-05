package db

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DBConnector {


  /*
      Returns a string used for connection to MongoDB
   */

  def createUri(
                 serverAddress: String,
                 db: String,
                 collection: String): String = {
    "mongodb://" + serverAddress + "/" + db + "." + collection
  }

  /*
    Creates a config for reading a MongoDB
   */

  def createReadConfig(inputUri: String, sparkSession: SparkSession): ReadConfig = {
    ReadConfig(Map("spark.mongodb.input.uri" -> inputUri, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkSession)))
  }

  /*
    Creates a config for writing in db, standard mode is overwrite, meaning DB gets dumped and rewritten as given Dataframe
   */

  def createWriteConfig(outputUri: String, replaceDocument: String = "false", mode: String = "overwrite", sparkSession: SparkSession): WriteConfig = {
    WriteConfig(Map("spark.mongodb.output.uri" -> outputUri, "replaceDocument" -> replaceDocument, "mode" -> mode), Some(WriteConfig(sparkSession)))

  }

  /*
    Reads the MongoDB specified in readConfig and returns it as RDD[Row]
   */

  def readFromDB(sparkSession: SparkSession, readConfig: ReadConfig): RDD[Row] = {
    val rdd = MongoSpark.load(sparkSession, readConfig).rdd
    if (rdd.isEmpty())
      throw new IllegalArgumentException("Empty DB")
    else
      rdd
  }

  /*
    Saves a dataframe to db specified in writeConfig
   */

  def writeToDB(savedInstance: DataFrame, writeConfig: WriteConfig): Unit = {
    MongoSpark.save(savedInstance, writeConfig)
  }


}

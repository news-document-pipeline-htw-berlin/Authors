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

  def createReadConfig(inputUri: String): ReadConfig = {
    ReadConfig(Map("spark.mongodb.input.uri" -> inputUri, "readPreference.name" -> "secondaryPreferred"))
  }

  /*
    Creates a config for writing in db, standard mode is overwrite, meaning DB gets dumped and rewritten as given Dataframe
   */

  def createWriteConfig(outputUri: String, replaceDocument: String = "false", mode: String = "overwrite"): WriteConfig = {
    WriteConfig(Map("spark.mongodb.output.uri" -> outputUri, "replaceDocument" -> replaceDocument, "mode" -> mode))

  }

  /*
    Reads the MongoDB specified in readConfig and returns it as RDD[Row]
   */

  def readFromDB(sparkSession: SparkSession, readConfig: ReadConfig): DataFrame = {
    val df = MongoSpark.load(sparkSession, readConfig)
    if (df.head(1).isEmpty) {
      throw new IllegalArgumentException("Provided Empty DB")
    }
    df
  }

  /*
    Saves a dataframe to db specified in writeConfig
   */

  def writeToDB(savedInstance: DataFrame, writeConfig: WriteConfig): Unit = {
    MongoSpark.save(savedInstance, writeConfig)
  }


}

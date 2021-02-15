package db

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import exceptions.EmptyDBException

/**
 * An object for standard interaction with SparkMongo
 */
object DBConnector {


  /**
   *
   * Returns a string used for connection to MongoDB
   *
   * @param serverAddress Address to connect to e.g. localhost
   * @param db            The MongoDB Database name to connect to
   * @param collection    The MongoDB Collection to connect to
   * @param port          The Port where MongoDB runs
   * @return a String composed  of  everything above to connect to a MongoDB Collection
   */
  def createUri(
                 serverAddress: String,
                 db: String,
                 collection: String,
                 port: String
               ): String = {
    "mongodb://" + serverAddress + ":" + port + "/" + db + "." + collection

  }


  /**
   * Creates a config for reading a MongoDB
   *
   * @param inputUri the URI where to find the desired MongoDB Collection to read from, best created with "createUri" but not necessary
   * @return a ReadConfig file instructing MongoSpark how to read from the inputUri
   */
  def createReadConfig(inputUri: String): ReadConfig = {
    ReadConfig(Map("spark.mongodb.input.uri" -> inputUri, "readPreference.name" -> "secondaryPreferred"))
  }


  /**
   * Creates a config for writing in db, standard mode is overwrite, meaning DB gets dumped and rewritten as given Dataframe
   *
   * @param outputUri the URI where to find the desired MongoDB Collection to write to, best created with "createUri" but not necessary
   * @param replaceDocument a boolean if the Document shall be replaced
   * @param mode how to write to the DB, standard is overwrite
   * @return a WriteConfig file instructing MongoSpark how to write to the outputUri
   */
  def createWriteConfig(outputUri: String, replaceDocument: String = "false", mode: String = "overwrite"): WriteConfig = {
    WriteConfig(Map("spark.mongodb.output.uri" -> outputUri, "replaceDocument" -> replaceDocument, "mode" -> mode))

  }


  /**
   * Reads the MongoDB specified in readConfig and returns it as DataFrame
   *
   * @param sparkSession a sparkSession (no context)
   * @param readConfig a ReadConfig instructing SparkMongo how to read the collection
   * @return a DataFrame from given data
   * @throws EmptyDBException when the DB is empty
   */
  def readFromDB(sparkSession: SparkSession, readConfig: ReadConfig): DataFrame = {
    val df = MongoSpark.load(sparkSession, readConfig)
    if (df.head(1).isEmpty) {
      throw EmptyDBException("Provided Empty DB")
    }
    df
  }



  /**
   * Saves a dataframe to DB specified in writeConfig

   *
   * @param data to be saved in collection from writeConfig
   * @param writeConfig to specify where to save the data
   */
  def writeToDB(data: DataFrame, writeConfig: WriteConfig): Unit = {
    MongoSpark.save(data, writeConfig)
  }


}

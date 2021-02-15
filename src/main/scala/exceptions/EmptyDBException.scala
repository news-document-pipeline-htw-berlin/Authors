package exceptions

/**
 * Is used when SparkMongo tries to return an Empty DB, because it otherwise doesn't check
 *
 * @param message
 * @param cause
 */
final case class EmptyDBException(private val message: String = "",
                                  private val cause: Throwable = None.orNull) extends Exception(message, cause)

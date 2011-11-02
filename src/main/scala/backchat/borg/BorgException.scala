package backchat.borg

/**
 * Base exception class from which all other Borg exceptions inherit.
 */
class BorgException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this() = this(null, null)
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(cause.getMessage, cause)
}
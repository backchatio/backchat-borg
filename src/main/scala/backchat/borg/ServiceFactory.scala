package backchat.borg

trait ServiceFactory extends (String ⇒ Option[Node])

object ServiceUnavailable extends ServiceFactory with Logging {
  def apply(service: String) = {
    logger warn "There was no service found for %s".format(service)
    None
  }
}
package events

//Typeclass induction
//https://gist.github.com/aaronlevin/d3911ba50d8f5253c85d2c726c63947b
//runMain events.Events
object Events {

  // Our events
  sealed trait Event
  case class Click(user: String, page: String)              extends Event
  case class Play(user: String, trackId: Long)              extends Event
  case class Pause(user: String, trackId: Long, ts: Long)   extends Event
  case class Forward(user: String, trackId: Long, ts: Long) extends Event
  case class ParseError(msg: String)                        extends Event
  // A type alias for "end of the type-level list"
  type EndOfList = Unit

  //TODO: Coproduct ???
  // list of events
  type Events = (Click, (Play, (Pause, (Forward, EndOfList))))

  // A typeclass for types that can be parsed from strings.
  trait FromString[E] {
    def fromString(s: String): Option[E]
  }

  // Parser instances for our types.
  implicit val clickFromstring = new FromString[Click] {
    def fromString(s: String): Option[Click] =
      s.split('\t').toList match {
        case user :: track :: Nil ⇒ Some(Click(user, track))
        case _                    ⇒ None
      }
  }

  // A small helper
  def safeToLong(s: String): Option[Long] =
    try Some(s.toLong)
    catch { case _: java.lang.NumberFormatException ⇒ None }

  implicit val playFromString = new FromString[Play] {
    def fromString(s: String): Option[Play] =
      s.split('\t').toList match {
        case user :: track :: Nil ⇒ safeToLong(track).map(Play(user, _))
        case _                    ⇒ None
      }
  }

  implicit val pauseFromString = new FromString[Pause] {
    def fromString(s: String): Option[Pause] =
      s.split('\t').toList match {
        case user :: track :: ts :: Nil ⇒
          safeToLong(track).flatMap(t ⇒ safeToLong(ts).map(Pause(user, t, _)))
        case _ ⇒ None
      }
  }

  implicit val forwardFromString = new FromString[Forward] {
    def fromString(s: String): Option[Forward] =
      s.split('\t').toList match {
        case user :: track :: ts :: Nil ⇒
          safeToLong(track).flatMap(t ⇒ safeToLong(ts).map(Forward(user, t, _)))
        case _ ⇒ None
      }
  }

  // A typeclass to extract names from a list of events.
  trait Named[E] {
    val name: String
  }

  // instances of Named for our events
  implicit val namedClick   = new Named[Click] { val name: String = "click" }
  implicit val namedPlay    = new Named[Play] { val name: String = "play" }
  implicit val namedPause   = new Named[Pause] { val name: String = "pause" }
  implicit val forwardPause = new Named[Forward] { val name: String = "forward" }

  // Named: base case
  implicit val baseCaseNamed = new Named[EndOfList] {
    val name: String = ""
  }

  // Named induction step: (E, Tail)
  implicit def inductionStepNamed[E, Tail](implicit
    n: Named[E],
    tailNames: Named[Tail]
  ): Named[(E, Tail)] =
    new Named[(E, Tail)] {
      override val name: String = s"${n.name}, ${tailNames.name}"
    }

  def getNamed[E](implicit names: Named[E]): String = names.name

  /** * Parsing Events / Dynamic Dispatch **
    */
  // A Typeclass for dynamic-dispatch on events
  trait HandleEvents[Events] {
    type Out
    def handleEvent(eventName: String, payload: String): Either[String, Out]
  }

  // HandleEvents: base case
  implicit val baseCaseHandleEvents = new HandleEvents[EndOfList] {
    type Out = Nothing
    def handleEvent(eventName: String, payload: String) = Left(s"Did not find event $eventName")
  }

  // HandleEvents: induction step (E, Tail)
  implicit def inductionStepHandleEvents[E, Tail](implicit
    namedEvent: Named[E],
    fromString: FromString[E],
    tailHandles: HandleEvents[Tail]
  ): HandleEvents[(E, Tail)] =
    new HandleEvents[(E, Tail)] {

      type Out = Either[tailHandles.Out, E]

      def handleEvent(eventName: String, payload: String): Either[String, Out] = {
        println(s"check [$eventName: $payload] against ${namedEvent.name}")
        if (eventName == namedEvent.name) {
          println(s"detect ${namedEvent.name}")
          fromString.fromString(payload) match {
            case None    ⇒ Left(s"""Could not decode event "$eventName" with payload "$payload"""")
            case Some(e) ⇒ Right(Right(e))
          }
        } else
          tailHandles.handleEvent(eventName, payload) match {
            case Left(e)  ⇒ Left(e)
            case Right(e) ⇒ Right(Left(e))
          }
      }
    }

  def handleEvent[Events](eventName: String, payload: String)(implicit
    names: HandleEvents[Events]
  ): Either[String, names.Out] = names.handleEvent(eventName, payload)

  def unfold(r: Any): Event =
    r match {
      case Left(r)       ⇒ unfold(r)
      case Right(r)      ⇒ unfold(r)
      case event: Event  ⇒ event
      case error: String ⇒ ParseError(error)
    }

  def main(args: Array[String]): Unit = {
    //handleEvent[Events]("click", "lambdaworld\tpage/rules"))
    //handleEvent[Events]("play", "lambdaworld\t123")
    //
    val r = handleEvent[Events]("pause", "lambdaworld\t123\t456")
    // handleEvent[Events]("forward", "lambdaworld\t123\t456")
    println(unfold(r))

  }
}

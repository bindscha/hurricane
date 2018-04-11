package ch.epfl.labos.hurricane

package object util {

  import scala.language.implicitConversions

  implicit def asFiniteDuration(d: java.time.Duration): scala.concurrent.duration.FiniteDuration =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

}

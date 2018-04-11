package ch.epfl.labos.hurricane.util

import akka._
import akka.actor._

import scala.collection._
import scala.concurrent._

case class Completed(id: String)

object CompletionWatcher {

  def watch()(implicit dispatcher: ExecutionContext): (Promise[Done], Future[Done]) = {
    val promise = Promise[Done]()
    val future = promise.future
    (promise, future)
  }

  def watch(n: Int)(implicit dispatcher: ExecutionContext): (Seq[Promise[Done]], Future[Done]) = {
    val promises = (0 until n) map (_ => Promise[Done]())
    val tmp = Future.foldLeft(promises.map(_.future))(Done)((a,_) => a)
    (promises, tmp)
  }

}

class CompletionWatcher(ids: Iterable[String], promise: Promise[Done]) extends Actor {

  val toWatch = mutable.Set.empty[String]

  override def preStart(): Unit = {
    super.preStart()

    ids foreach ( toWatch += _ )
  }

  override def postStop(): Unit = {
    promise.success(Done)

    super.postStop()
  }

  override def receive = {
    case Completed(id) =>
      toWatch -= id
      if(toWatch.isEmpty) {
        context stop self
      }
  }
}
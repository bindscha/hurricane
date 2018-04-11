package ch.epfl.labos.hurricane.util

import akka.actor._
import akka._

import scala.collection._
import scala.concurrent._

object ActorWatcher {

  def watch(refs: Iterable[ActorRef])(implicit context: ActorContext): Future[Done] =
  if (refs.isEmpty) {
    Future.successful(Done)
  } else {
    val promise = Promise[Done]()
    context.actorOf(Props(classOf[ActorWatcher], refs, promise))
    promise.future
  }

}

class ActorWatcher(refs: Iterable[ActorRef], promise: Promise[Done]) extends Actor {

  val toWatch = mutable.Set.empty[ActorRef]

  override def preStart(): Unit = {
    super.preStart()

    refs foreach { ref =>
      toWatch += ref
      context watch ref
    }
  }

  override def postStop(): Unit = {
    promise.success(Done)

    super.postStop()
  }

  override def receive = {
    case Terminated(ref) =>
      toWatch -= ref
      if(toWatch.isEmpty) {
        context stop self
      }
  }
}
package ch.epfl.labos.hurricane.frontend

import akka._
import akka.actor._
import akka.pattern._
import akka.stream._
import akka.stream.actor._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.Timeout
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.serialization.Format
import ch.epfl.labos.hurricane.util._

import scala.collection.immutable._
import scala.concurrent._
import scala.language.postfixOps
import scala.util._

object HurricaneGraph {

  def source(bag: Bag)(implicit dispatcher: ExecutionContext): Source[Chunk, Future[Done]] = {
    val (promise, future) = CompletionWatcher.watch()
    Source.actorPublisher(Filler.props(bag, Some(promise))).mapMaterializedValue(_ => future)
  }

  def sources(bags: Seq[Bag])(implicit dispatcher: ExecutionContext): Source[Seq[Chunk], Future[Done]] = {
    val (promises, future) = CompletionWatcher.watch(bags.size)
    val ss = (bags zip promises) map (pb => source(pb._1) mapMaterializedValue { f => pb._2.completeWith(f); f })
    Source.zipN(ss).mapMaterializedValue(_ => future)
  }

  def sink(bag: Bag)(implicit dispatcher: ExecutionContext): Sink[Chunk, Future[Done]] = {
    val (promise, future) = CompletionWatcher.watch()
    Sink.actorSubscriber(Drainer.props(bag, Some(promise))).mapMaterializedValue(_ => future)
  }

  /*def readFully[I: Format](bag: Bag)(implicit dispatcher: ExecutionContext): Future[Iterator[I]] = {
    implicit val materializer = ActorMaterializer()
    source(bag).map(_.iterator[I](implicitly[Format[I]])).runWith(Sink.head)
  }*/

  def map[In: Format, Out: Format](f: In => Out) = new Transform[In,Out](_ map f)

  def transform[In: Format, Out: Format](f: Iterator[In] => Iterator[Out]) = new Transform[In,Out](f)

  def splitBy[In: Format](k: Int, f: In => Int) = new SplitBy[In](k, f)

  def groupBy[In: Format, Out: Format](state: GroupByState[In, Out], valueParser: Option[In => (In, Out)] = None) = new GroupBy[In, Out](state, valueParser)

  def join[In: Format, Out: Format](state: JoinState[In, Out]) = new Join[In, Out](state)

  def reduce[In: Format, Out: Format](f: (Out, In) => Out) = new Reduce[In,Out](f)

  def sourceToSinks[Mat](indexedSource: Graph[SourceShape[(FlowIndex, Chunk)], Mat], bags: Seq[Bag])(implicit dispatcher: ExecutionContext): RunnableGraph[Future[Done]] = {
    val (promises, future) = CompletionWatcher.watch(bags.size)
    RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val src = builder.add(indexedSource)
      val bcast = builder.add(Broadcast[(FlowIndex, Chunk)](bags.size))
      val sinks = bags zip promises map ( pb =>
        builder.add(Sink.actorSubscriber(Drainer.props(pb._1, Some(pb._2))))
      )

      src ~> bcast

      sinks.zipWithIndex foreach { indexedSink =>
        bcast.filter(_._1 == indexedSink._2).map(_._2) ~> indexedSink._1
      }

      ClosedShape
    }).mapMaterializedValue { _ => future }
  }

  def flowToSinks[In, Mat](indexedFlow: Graph[FlowShape[In, (FlowIndex, Chunk)], Mat], bags: Seq[Bag])(implicit dispatcher: ExecutionContext): Sink[In, Future[Done]] = {
    val (promises, future) = CompletionWatcher.watch(bags.size)
    Sink.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val upstream = builder.add(indexedFlow)
      val bcast = builder.add(Broadcast[(FlowIndex, Chunk)](bags.size))
      val sinks = bags zip promises map ( pb =>
        builder.add(Sink.actorSubscriber(Drainer.props(pb._1, Some(pb._2))))
        )

      upstream.out ~> bcast

      sinks.zipWithIndex foreach { indexedSink =>
        bcast.filter(_._1 == indexedSink._2).map(_._2) ~> indexedSink._1
      }

      SinkShape(upstream.in)
    }).mapMaterializedValue { _ => future }
  }

}

object Filler {
  def props(bag: Bag, completionPromise: Option[Promise[Done]] = None): Props = Props(classOf[Filler], bag.id, completionPromise) // Unpacking manually because bag is value class

  implicit val timeout = Timeout(Config.HurricaneConfig.FrontendConfig.sourceRetry)

  case class Acked(id: ActorCounter.Id, chunk: Chunk)
  case class Nacked(id: ActorCounter.Id, ref: ActorRef)
  case class Retry(id: ActorCounter.Id)
  case object Finish

}

class Filler(bag: Bag, completionPromise: Option[Promise[Done]] = None) extends ActorPublisher[Chunk] with ActorLogging with ActorCounter {
  import Filler._
  import context.dispatcher

  import scala.collection.mutable.{Map => MMap}

  val cyclic =
    Config.HurricaneConfig.FrontendConfig.ioMode match {
      case Config.HurricaneConfig.FrontendConfig.InputLocal => Cyclic[ActorRef](1, Seq(Config.HurricaneConfig.BackendConfig.NodesConfig.backendRefs(bag.id.hashCode % Config.HurricaneConfig.BackendConfig.NodesConfig.machines)))
      case Config.HurricaneConfig.FrontendConfig.OutputLocal => Cyclic[ActorRef](7 * bag.id.hashCode + 13 * Config.HurricaneConfig.me, Config.HurricaneConfig.BackendConfig.NodesConfig.backendRefs)
      case Config.HurricaneConfig.FrontendConfig.Spreading => Cyclic[ActorRef](7 * bag.id.hashCode + 13 * Config.HurricaneConfig.me, Config.HurricaneConfig.BackendConfig.NodesConfig.backendRefs)
    }

  val fingerprint = java.util.UUID.nameUUIDFromBytes(Config.HurricaneConfig.FrontendConfig.NodesConfig.localNode.toString.getBytes).toString

  val done = MMap(cyclic.permutation.map(_ -> false) : _*)

  var buffer = SortedMap.empty[ActorCounter.Id, Option[Chunk]]
  var lastDeliveredId = 0
  var pending = 0L

  override def preStart(): Unit = {
    log.info("Started filler for bag " + bag.id)
    deliver()

    super.preStart()
  }

  override def postStop(): Unit = {
    log.info("Stopped filler for bag " + bag.id)

    super.postStop()
  }

  def close() = {
    completionPromise foreach { _.success(Done) }
    context stop self
  }

  override def receive = {
    case Acked(id, chunk) =>
      buffer += id -> Some(chunk)
      pending -= 1
      deliver()

    case Nacked(id, ref) =>
      buffer += id -> None
      done += ref -> true
      pending -= 1
      deliver()
      if(done.values forall identity) {
        onComplete()
        close()
      }

    case Retry(id) =>
      log.info(s"Rereading $id for bag ${bag.id} due to failure...")
      pending -= 1
      fillChunk(id)

    case ActorPublisherMessage.Request(_) =>
      deliver()

    case ActorPublisherMessage.Cancel =>
      deliver()
      close()
  }

  private def deliverableCount: Int = {
    def innerDeliverable(last: ActorCounter.Id, iter: Iterator[ActorCounter.Id]): Int =
      if (iter.hasNext) {
        val next = iter.next
        if (next == last + 1) 1 + innerDeliverable(next, iter) else 0
      } else {
        0
      }

    innerDeliverable(lastDeliveredId, buffer.keysIterator)
  }

  def deliver(): Unit = {
    if (totalDemand > 0) {
      val deliverable = deliverableCount
      val toDeliver = if (totalDemand < deliverable) totalDemand.toInt else deliverable
      val (use, keep) = buffer.splitAt(toDeliver)
      buffer = keep
      lastDeliveredId += use.size
      use.values.flatten foreach onNext
    }

    ((buffer.size + pending) until Config.HurricaneConfig.FrontendConfig.sourceBufferSize) foreach { _ => fillChunk(nextId) }
  }

  def fillChunk(id: ActorCounter.Id): Unit = {
    val me = self
    val target = cyclic.next
    pending += 1
    (target ? Fill(fingerprint, bag)) onComplete {
      case Success(Filled(chunk)) => me ! Acked(id, chunk)
      case Success(EOF) => me ! Nacked(id, target)
      case _ => me ! Retry(id)
    }
  }

}

object Drainer {

  def props(bag: Bag, completionPromise: Option[Promise[Done]] = None): Props = Props(classOf[Drainer], bag.id, completionPromise) // Unpacking manually because bag is value class

  implicit val timeout = Timeout(Config.HurricaneConfig.FrontendConfig.sinkRetry)

  case class Acked(id: ActorCounter.Id)
  case class Retry(id: ActorCounter.Id)
  case object Finish

}

class Drainer(bag: Bag, completionPromise: Option[Promise[Done]] = None) extends ActorSubscriber with ActorLogging with ActorCounter {
  import Drainer._
  import context.dispatcher

  val cyclic =
    Config.HurricaneConfig.FrontendConfig.ioMode match {
      case Config.HurricaneConfig.FrontendConfig.InputLocal => Cyclic[ActorRef](7 * bag.id.hashCode + 13 * Config.HurricaneConfig.me, Config.HurricaneConfig.BackendConfig.NodesConfig.backendRefs)
      case Config.HurricaneConfig.FrontendConfig.OutputLocal => Cyclic[ActorRef](1, Seq(Config.HurricaneConfig.BackendConfig.NodesConfig.backendRefs(bag.id.hashCode % Config.HurricaneConfig.BackendConfig.NodesConfig.machines)))
      case Config.HurricaneConfig.FrontendConfig.Spreading => Cyclic[ActorRef](7 * bag.id.hashCode + 13 * Config.HurricaneConfig.me, Config.HurricaneConfig.BackendConfig.NodesConfig.backendRefs)
    }

  val fingerprint = java.util.UUID.nameUUIDFromBytes(Config.HurricaneConfig.FrontendConfig.NodesConfig.localNode.toString.getBytes).toString

  var queue = Map.empty[ActorCounter.Id, Chunk]

  override val requestStrategy =
    new MaxInFlightRequestStrategy(max = Config.HurricaneConfig.FrontendConfig.sinkQueueSize) {
      override def inFlightInternally: Int = queue.size
    }

  override def preStart(): Unit = {
    super.preStart()

    log.info("Started drainer for bag " + bag.id)
  }

  override def postStop(): Unit = {
    log.info("Stopped drainer for bag " + bag.id)

    super.postStop()
  }

  def close() = {
    log.info("Flushing bag " + bag.id)
    val me = self
    Future.foldLeft(cyclic.permutation.map(_ ? Flush(fingerprint, bag)))(Future.unit)((a,b) => a) onComplete { _ =>
      completionPromise foreach { _.success(Done) }
      me ! PoisonPill
    }
  }

  override def receive = {
    case Acked(id) =>
      queue -= id
      if (canceled && queue.isEmpty) {
        close()
      }

    case Retry(id) if queue.contains(id) =>
      log.info(s"Retransmitting $id for bag ${bag.id} due to failure...")
      drainChunk(id, queue(id))

    case ActorSubscriberMessage.OnNext(chunk: Chunk) =>
      val id = nextId
      queue += id -> chunk
      drainChunk(id, chunk)

    case ActorSubscriberMessage.OnComplete =>
      if (queue.isEmpty) {
        close()
      }
      
    case ActorSubscriberMessage.OnError(cause) =>
      throw cause
  }

  def drainChunk(id: ActorCounter.Id, chunk: Chunk): Unit = {
    val me = self
    val target = cyclic.next
    (target ? Drain(fingerprint, bag, chunk)) onComplete {
      case Success(Ack) => me ! Acked(id)
      case _ => me ! Retry(id)
    }
  }

}

class Buffer[I: Format] {
  var buffer = ChunkPool.allocate()
  var pusher = buffer.pusher[I]

  def put(item: I): Option[Chunk] = {
    val res = pusher.put(item)
    if(res >= 0) {
      None
    } else {
      val ret = buffer
      buffer.chunkSize(pusher.pushed)
      buffer = ChunkPool.allocate()
      pusher = buffer.pusher[I]
      pusher.put(item)
      Some(ret)
    }
  }

  def finish: Iterator[Chunk] = {
    buffer.chunkSize(pusher.pushed)
    Iterator.single(buffer)
  }

}

class Transform[In: Format, Out: Format](f: Iterator[In] => Iterator[Out]) extends GraphStage[FlowShape[Chunk, Chunk]] {

  val in = Inlet[Chunk]("Transform.in")
  val out = Outlet[Chunk]("Transform.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var buffer = new Buffer[Out]

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val full = f(grab(in).iterator[In]) flatMap buffer.put
          if(full.hasNext) {
            emitMultiple(out, full)
          } else {
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {
          emitMultiple(out, buffer.finish)
          complete(out)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}

class Buffers[I: Format](k: Int) {
  var buffers: Array[Chunk] = 0 until k map (_ => ChunkPool.allocate()) toArray
  var pushers: Array[Pusher[I]] = buffers map (_.pusher[I])

  def put(i: FlowIndex, item: I): Option[(FlowIndex, Chunk)] = {
    val res = pushers(i).put(item)
    if(res >= 0) {
      None
    } else {
      val ret = buffers(i)
      ret.chunkSize(pushers(i).pushed)
      buffers(i) = ChunkPool.allocate()
      pushers(i) = buffers(i).pusher[I]
      pushers(i).put(item)
      Some(i -> ret)
    }
  }

  def finish: Iterator[Chunk] = {
    (buffers.indices zip buffers).iterator.map { case (i, b) => b.chunkSize(pushers(i).pushed); b }
  }

  def finishWithIndex: Iterator[(FlowIndex,Chunk)] =
    (buffers.indices zip buffers).iterator.map { case (i, b) => b.chunkSize(pushers(i).pushed); i -> b }
}

class SplitBy[In: Format](k: Int, f: In => Int) extends GraphStage[FlowShape[Chunk, (FlowIndex, Chunk)]] {

  val in = Inlet[Chunk]("SplitBy.in")
  val out = Outlet[(FlowIndex, Chunk)]("SplitBy.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      val buffers = new Buffers[In](k)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val full = grab(in).iterator[In] flatMap { item => buffers.put(f(item), item) }
          if(full.hasNext) {
            emitMultiple(out, full)
          } else {
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {
          emitMultiple(out, buffers.finishWithIndex)
          complete(out)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}

trait GroupByState[Key, Value] {

  def update(item: Key): Any

  def updateValue(item: Key, value: Value): Any

  def chunkIterator: Iterator[Chunk]

}

class GroupBy[Key: Format, Value: Format]
  (state: GroupByState[Key, Value], valueParser: Option[Key => (Key, Value)] = None) extends GraphStage[FlowShape[Chunk, Chunk]] {

  val in = Inlet[Chunk]("GroupBy.in")
  val out = Outlet[Chunk]("GroupBy.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          valueParser match {
            case Some(vp) => grab(in).iterator[Key].map(vp) foreach { case (k, v) => state.updateValue(k, v) }
            case None => grab(in).iterator[Key] foreach state.update
          }
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          emitMultiple(out, state.chunkIterator)
          complete(out)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}

trait JoinState[In, Out] {

  def offer(item: In): Option[Out]

}

class Join[In: Format, Out: Format]
  (state: JoinState[In, Out]) extends GraphStage[FlowShape[Chunk, Chunk]] {

  val in = Inlet[Chunk]("Join.in")
  val out = Outlet[Chunk]("Join.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var buffer = new Buffer[Out]

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val full = grab(in).iterator[In] flatMap state.offer flatMap buffer.put
          if(full.hasNext) {
            emitMultiple(out, full)
          } else {
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {
          emitMultiple(out, buffer.finish)
          complete(out)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}

class Reduce[In: Format, Out: Format](f: (Out, In) => Out) extends GraphStage[FlowShape[Seq[Chunk], Chunk]] {

  val in = Inlet[Seq[Chunk]]("Reduce.in")
  val out = Outlet[Chunk]("Reduce.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var buffer = new Buffer[Out]

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val full = grab(in).map(_.iterator[In]).foldLeft(ChunkPool.allocate().iterator[Out])((it1, it2) => (it1 zip it2).map(p => f(p._1, p._2))) flatMap buffer.put
          if(full.hasNext) {
            emitMultiple(out, full)
          } else {
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {
          emitMultiple(out, buffer.finish)
          complete(out)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}

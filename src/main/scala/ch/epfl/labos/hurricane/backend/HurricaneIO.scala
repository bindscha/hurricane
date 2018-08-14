package ch.epfl.labos.hurricane.backend

import java.io.{File => _, _}
import java.nio.ByteBuffer
import akka.actor._
import better.files.File
import ch.epfl.labos.hurricane._
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.frontend.Statistics
import net.smacke.jaydio._
import com.twitter.zk.ZkClient
import ch.epfl.labos.hurricane.test.ZK
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}

object HurricaneIO {

  def props(bag: Bag, address: String, zkClient: ZkClient, root: File): Props =
    Config.HurricaneConfig.BackendConfig.DataConfig.ioEngine match {
      case Config.HurricaneConfig.BackendConfig.DataConfig.DefaultIOEngine =>
        Props(classOf[HurricaneIO], bag.id, address, zkClient, root).withDispatcher("hurricane.backend.blocking-io-dispatcher")
      case Config.HurricaneConfig.BackendConfig.DataConfig.DirectIOEngine =>
        Props(classOf[HurricaneDIO], bag.id, address, zkClient, root).withDispatcher("hurricane.backend.blocking-io-dispatcher")
    }

  sealed trait Operation

  case object Read extends Operation

  case object Write extends Operation

  case object Noop extends Operation

  /*def main(args: Array[String]): Unit = {
    println("Hurricane IO test")

    val size = 10L * 1024 * 1024 * 1024
    val chunk = 4 * 1024 * 1024
    val f = new RandomAccessFile(File("test").toJava, "rw")

    val serializer = new ch.epfl.labos.hurricane.serialization.WireSerializer

    val buf = ByteBuffer.allocate(2 * chunk)

    val start = System.nanoTime
    for(i <- 0 until (size / chunk).toInt) {
      println(i)
      val buffer = new Array[Byte](chunk)
      val drain = Drain(Bag.random, buffer)
      serializer.toBinary(drain, buf)
      buf.rewind()
      serializer.fromBinary(buf, "")
      buf.rewind()
      val array = new Array[Byte](buf.remaining)
      serializer.toBinary(drain, buf)
      f.write(array)
    }
    f.close()
    val stop = System.nanoTime

    val runtime = (stop - start) * 0.000000001

    println("Runtime:" + runtime + " seconds")
    println("Bandwidth: " + (size * 0.000001) / runtime + " MB/s")
  }*/
}

class HurricaneIO(bag: Bag, address: String, zkClient: ZkClient, root: File = File(Config.HurricaneConfig.BackendConfig.DataConfig.dataDirectory.getAbsolutePath)) extends Actor with ActorLogging {

  import HurricaneIO._

  // Create root if not exists
  root.createDirectories()

  private var inout = new RandomAccessFile((root / bag.id).toJava, "rw")

  def receive = {
    case Create(fingerprint, file) =>
      // do nothing
    case Fill(fingerprint, file, count) =>
    //case Fill(workerId, file, count) =>
      val buffer = ChunkPool.allocate()
      val fp = inout.getFilePointer
      val read = withStats(Read) {
        val ret = inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
        ret
      }
      if (read >= 0) {
        //val parentNode = "/" + workerId + "~" + bag.id
        val parentNode = "/" + fingerprint + "~" + "input"
        //Create parent node if not exists
        if (!ZK.checkPathExistExample(zkClient, parentNode)) {
          try {
            ZK.createPathExample(zkClient, parentNode, new Array[Byte](0))
          } catch {
            case nee: NodeExistsException => "Other storage nodes might create the path at the same time"
          }
        }

        val leafNode = parentNode + "/" + address
        //Create leaf node if not exists, store file pointer(offset) to the leaf node
        if (!ZK.checkPathExistExample(zkClient, leafNode)) {
          val buf = ByteBuffer.wrap(new Array[Byte](8))
          buf.putLong(fp)
          ZK.createPathExample(zkClient, leafNode, buf.array)
        } else {
          //Update file pointer(offset) to the leaf node(needs to be nodefiend)
          val offsetBuffer = ByteBuffer.wrap(new Array[Byte](8))
          offsetBuffer.putLong(fp)
          val bufArray = ZK.getDataExample(zkClient, leafNode) ++ offsetBuffer.array
          ZK.setDataExample(zkClient, leafNode, bufArray)
        }

        /*
        //test here
        val bytes = ZK.getDataExample(zkClient, leafNode)
        println(address + " start")
        ZK.printOffset(bytes)
        println(address + " end ")
        */

        buffer.chunkSize(read)
        sender ! Filled(buffer)
      } else {
        sender ! EOF
      }

    case Replay(fingerprint: FingerPrint, bag: Bag) =>
    //case ReplayFill(failedWorkerId, workerId, file, count) =>
      val failedWorkerId = ""
      println("xinjaguo replay starts!")
      //val failedLeafNode = "/" + failedWorkerId + "~" + bag.id +  "/" + address
      val failedLeafNode = "/" + failedWorkerId + "~" + "input" +  "/" + address
      println("xinjaguo failed leaf node: " + failedLeafNode)
      if(!ZK.checkPathExistExample(zkClient, failedLeafNode)){
        println("xinjaguo replay find path")
        val bytes = ZK.getDataExample(zkClient, failedLeafNode)
        if (bytes.length >= 8) {  //at least 1 offset stored in the leaf node
          val buffer = ChunkPool.allocate()
          val fp = inout.getFilePointer
          val buf = ByteBuffer.wrap(bytes)
          val offset = buf.getLong(0)
          val read = withStats(Read) {
            inout.seek(offset)
            val ret = inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
            inout.seek(fp)
            ret
          }
          if(read >= 0){
            //Delete the offset from failed worker leaf node
            val newBytes = bytes.slice(8, bytes.length)
            ZK.setDataExample(zkClient, failedLeafNode, newBytes)

            //Add the offset to the current worker leaf node
            //val parentNode = "/" + workerId + "~" + bag.id
            val parentNode = "/" + fingerprint + "~" + "input"
            //Create parent node if not exists
            if(!ZK.checkPathExistExample(zkClient, parentNode)){
              try {
                ZK.createPathExample(zkClient, parentNode, new Array[Byte](0))
              }catch {
                case nee : NodeExistsException => "Other storage nodes might create the path at the same time"
              }
            }

            val leafNode = parentNode + "/" + address
            //Create leaf node if not exists, store file pointer(offset) to the leaf node
            if(!ZK.checkPathExistExample(zkClient, leafNode)){
              val buf = ByteBuffer.wrap(new Array[Byte](8))
              buf.putLong(offset)
              ZK.createPathExample(zkClient, leafNode, buf.array)
            }else{
              //Update file pointer(offset) to the leaf node
              val offsetBuffer = ByteBuffer.wrap(new Array[Byte](8))
              offsetBuffer.putLong(offset)
              val bufArray = ZK.getDataExample(zkClient, leafNode) ++ offsetBuffer.array
              ZK.setDataExample(zkClient, leafNode, bufArray)
            }

            buffer.chunkSize(read)
            sender ! Filled(buffer)
          }else{
            sender ! EOF
          }




        }else{
          // no more data to get
        }

      }else{
        sender ! EOF
      }

    case SeekAndFill(fingerprint, file, offset, count) =>
      val buffer = ChunkPool.allocate()
      val fp = inout.getFilePointer
      val read = withStats(Read) {
        inout.seek(offset)
        val ret = inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
        inout.seek(fp)
        ret
      }
      if(read >= 0) {
        buffer.chunkSize(read)
        sender ! Filled(buffer)
      } else {
        sender ! EOF
      }
    case Drain(fingerprint, file, data) =>
      withStats(Write) {
        inout.write(data.array, 0, data.chunkSize)
      }
      sender ! Ack
    case Rewind(fingerprint, file) =>
      withStats(Noop) {
        inout.seek(0L)
      }
      sender ! Ack
    case Trunc(fingerprint, file) =>
      withStats(Noop) {
        inout.close()
        (root / bag.id).delete(true)
        inout = new RandomAccessFile((root / bag.id).toJava, "rw")
      }
      sender ! Ack
    case Flush(fingerprint, file) =>
      withStats(Noop) {
        inout.getFD.sync()
      }
      sender ! Ack
    case Progress(fingerprint, file) =>
      val done = if(inout.length == 0) 1.0 else inout.getFilePointer.toDouble / inout.length.toDouble
      sender ! ProgressReport(done, inout.length)
  }

  def withStats[A](op: Operation)(f: => A): A = {
    val started = System.nanoTime()
    val ret = f
    Statistics.ioTime send (_ + (System.nanoTime - started))
    op match {
      case Read if ret.asInstanceOf[Int] > 0 => Statistics.chunksRead send (_ + 1)
      case Write => Statistics.chunksWritten send (_ + 1)
      case _ =>
    }
    ret
  }

}

class HurricaneDIO(bag: Bag, root: File = File(Config.HurricaneConfig.BackendConfig.DataConfig.dataDirectory.getAbsolutePath)) extends Actor with ActorLogging {

  import HurricaneIO._

  // Create root if not exists
  root.createDirectories()

  private var inout = new DirectRandomAccessFile((root / bag.id).toJava, "rw", 4 * 1024 * 1024)

  // XXX: read is a problem if it does not have exactly the right amount (e.g., last chunk of file)

  def receive = {
    case Create(fingerprint, file) =>
    // do nothing
    case Fill(fingerprint, file, count) =>
      val buffer = ChunkPool.allocate()
      withStats(Read) {
        inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
        buffer.array.length
      }
      sender ! Filled(buffer)
    case SeekAndFill(fingerprint, file, offset, count) =>
      val buffer = ChunkPool.allocate()
      val fp = inout.getFilePointer
      withStats(Read) {
        inout.seek(offset)
        inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
        inout.seek(fp)
        buffer.array.length
      }
      sender ! Filled(buffer)
    case Drain(fingerprint, file, data) =>
      withStats(Write) {
        inout.write(data.array, 0, data.chunkSize)
        Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize
      }
      sender ! Ack
    case Rewind(fingerprint, file) =>
      withStats(Noop) {
        inout.seek(0L)
      }
      sender ! Ack
    case Trunc(fingerprint, file) =>
      withStats(Noop) {
        inout.close()
        (root / bag.id).delete(true)
        inout = new DirectRandomAccessFile((root / bag.id).toJava, "rw", 4 * 1024 * 1024)
      }
      sender ! Ack
    case Flush(fingerprint, file) => // no need to flush, but we still ack it
      sender ! Ack
    case Progress(fingerprint, file) =>
      val done = if(inout.length == 0) 1.0 else inout.getFilePointer.toDouble / inout.length.toDouble
      sender ! ProgressReport(done, inout.length)
  }

  def withStats[A](op: Operation)(f: => A): A = {
    val started = System.nanoTime()
    val ret = f
    Statistics.ioTime send (_ + (System.nanoTime - started))
    op match {
      case Read if ret.asInstanceOf[Int] > 0 => Statistics.chunksRead send (_ + 1)
      case Write => Statistics.chunksWritten send (_ + 1)
      case _ =>
    }
    ret
  }

}

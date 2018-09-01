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
import ch.epfl.labos.hurricane.test.ZK.getReference
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
      //Step1: Read chunk from Replay path if Replay path exists and store the chunk in the Record path
      val replayPath = "/replay/" + fingerprint + "~" + bag.id + "/" + address
      if (ZK.checkPathExistExample(zkClient, replayPath)) {
        println("Read From Replay: " + replayPath)
        val bytes = ZK.getDataExample(zkClient, replayPath)
        val buf = ByteBuffer.wrap(bytes)
        val offset = buf.getLong(0)
        val newBytes = bytes.slice(8, bytes.length)

        if(newBytes.length >= 8) {
          //Update the replay path with size - 8 bytes(1 chunk)
          ZK.setDataExample(zkClient, replayPath, newBytes)
        }else{
          //No more offset to read, remove the replay path
          ZK.deletePathExample(zkClient, replayPath)
        }

        val buffer = ChunkPool.allocate()
        val fp = inout.getFilePointer
        val read = withStats(Read) {
          inout.seek(offset)
          val ret = inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
          //Set back file pointer
          inout.seek(fp)
          ret
        }
        if (read >= 0) {
          val parentNode = "/record/" + fingerprint + "~" + bag.id
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
            buf.putLong(offset)
            ZK.createPathExample(zkClient, leafNode, buf.array)
          } else {
            //Update file pointer(offset) to the leaf node(needs to be nodefiend)
            val offsetBuffer = ByteBuffer.wrap(new Array[Byte](8))
            offsetBuffer.putLong(offset)
            val bufArray = ZK.getDataExample(zkClient, leafNode) ++ offsetBuffer.array
            ZK.setDataExample(zkClient, leafNode, bufArray)
          }

          buffer.chunkSize(read)
          sender ! Filled(buffer)
        } else {
          sender ! EOF
        }

        //Step2: Read chunk from file if Replay path not exists, and store the chunk in the Record path
      }else{
        //if replay path not exists, read from file
        println("Read from file")
        val fp = inout.getFilePointer
        val buffer = ChunkPool.allocate()
        val read = withStats(Read) {
          val ret = inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
          ret
        }

        if (read >= 0) {
          //update offset to zookeeper storage side
          val parentNode = "/record/" + fingerprint + "~" + bag.id
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

          buffer.chunkSize(read)
          sender ! Filled(buffer)
        } else {
          sender ! EOF
        }
      }


    case Replay(oldWorker, newWorker, file) =>

      //Step1:Replace oldWorker by newWorker in the replay path
      val reference = ZK.getReference(zkClient, "/replay", oldWorker, newWorker, address)
      for ((oldPath, newPath) <- reference){
        val bytes = ZK.getDataExample(zkClient, oldPath)
        //check whether the layer3 path created
        if(ZK.checkPathExistExample(zkClient, newPath)) {
          val newBytes = ZK.getDataExample(zkClient, newPath) ++ bytes
          ZK.setDataExample(zkClient, newPath, newBytes)
        } else {
          //check whether the layer2 path created
          val index = newPath.lastIndexOf("/")
          val layer2Path = newPath.substring(0, index)
          if(!ZK.checkPathExistExample(zkClient, layer2Path)){
            //create layer2
            ZK.createPathExample(zkClient, layer2Path, new Array[Byte](0))
          }
          //create layer3
          ZK.createPathExample(zkClient, newPath, bytes)
        }
        //delete oldPath
        ZK.deletePathExample(zkClient, oldPath)
      }


      //Step2: Move chunks from oldWorker(Record) to newWorker(Replay)
      val oldLeafNode = "/record/" + oldWorker + "~" + bag.id + "/" + address
      if (ZK.checkPathExistExample(zkClient, oldLeafNode)) {
        //get the offset from old worker of record tree
        val bytes = ZK.getDataExample(zkClient, oldLeafNode)

        if (bytes.length >= 8) { //at least 1 offset stored in the record
          val newReplayParentNode = "/replay/" + newWorker + "~" + bag.id

          //Create parent node if not exists
          if(!ZK.checkPathExistExample(zkClient, newReplayParentNode)) {
            try {
              ZK.createPathExample(zkClient, newReplayParentNode, new Array[Byte](0))
            } catch {
              case nee: NodeExistsException => "Other storage nodes might create the path at the same time"
            }
          }

          val newReplayLeafNode = newReplayParentNode + "/" + address
          //Create leaf node if not exists, store file pointer(offset) to the leaf node
          if(!ZK.checkPathExistExample(zkClient, newReplayLeafNode)) {
            ZK.createPathExample(zkClient, newReplayLeafNode, bytes)
          }else {
            val newBytes = ZK.getDataExample(zkClient, newReplayLeafNode) ++ bytes
            ZK.setDataExample(zkClient, newReplayLeafNode, newBytes)
          }

          ZK.deletePathExample(zkClient, oldLeafNode)
        }else{
          // no more data to get
        }
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

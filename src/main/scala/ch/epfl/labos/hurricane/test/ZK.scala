package ch.epfl.labos.hurricane.test

import java.nio.ByteBuffer

import ch.epfl.labos.hurricane.serialization.WireSerializer.{COMMAND_LENGTH, FILL_COMMAND}
import com.twitter.zk.{ZkClient, _}
import com.twitter.util.{Await, JavaTimer}
import com.twitter.conversions.time._
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
import org.apache.zookeeper.data.ACL

import scala.collection.JavaConverters._


object ZK {
  def main(args: Array[String]): Unit = {

    //connections
    implicit val timer = new JavaTimer(true)
    val zkClient = ZkClient("localhost:2181", Some(5.seconds), 30.seconds)



    //val path = "/123e4567-e89b-12d3-a456-426655440000/127.0.0.1:2553"
    //val path = "/123e4567-e89b-12d3-a456-426655440000"
    //val path = "/13adb6eb-33db-4504-8a09-e421fc0567da~C:\\\\Users\\\\j84091920\\\\Desktop\\\\input"
    val path = "/13adb6eb-33db-4504-8a09-e421fc0567da~input"
    val childrenPath = "/13adb6eb-33db-4504-8a09-e421fc0567da~input/127.0.0.1:2554"
    val newPath = "/24adb6ee-33db-4504-8a09-e421fc0567da~input/127.0.0.1:2554"
    //val path = "/test"
    //var version = -1

    //val buf = ByteBuffer.wrap(new Array[Byte](24))
    //buf.putLong(0, 20)
    //buf.putLong(8, 4543)
    //buf.putLong(16, 255)

    //create path
    //createPathExample(zkClient, path, buf.array)
    //version = getDataVersionExample(zkClient, path)

    //set data to a existing path
    //setDataExample(zkClient, path, buf.array())

    //read data from a existing path
    val bytes = getDataExample(zkClient, newPath)
    printOffset(bytes)



    //println("xinjaguo first: " + first)
    //println("xinjaguo second: " + second)
    //println("xinjaguo third: " + third)
    //println("xinjaguo fourth: " + fourth)


    //delete path
    //getChidrenExample(zkClient, path)
    //deletePathExample(zkClient, path)

    //check path exist
    //val exist = checkPathExistExample(zkClient, path)
    //println("xinjaguo exist: " + exist)


  }

  def printOffset(bytes : Array[Byte], start : Int = 0) : Unit = {
    val buf = ByteBuffer.wrap(bytes)
    var i = start
    while(i < bytes.length){
      println("index at " + i + ": " + buf.getLong(i))
      i = i + 8
    }
  }

  @throws(classOf[NodeExistsException])
  def createPathExample(zkClient: ZkClient, path : String, data : Array[Byte]) : Unit = {
    val acl: Seq[ACL] = OPEN_ACL_UNSAFE.asScala
    //val buf = ByteBuffer.wrap(new Array[Byte](4))
    //buf.putInt(data)
    val mode = zkClient.mode
    val create = zkClient(path).create(data, acl, mode)
    val res = Await.result(create)
  }

  @throws(classOf[NoNodeException])
  def deletePathExample(zkClient: ZkClient, path : String) : Unit = {
    //znode can be deleted only if all its children are deleted
    val version = getDataVersionExample(zkClient, path)
    val delete = zkClient(path).delete(version)
    val res = Await.result(delete)
  }

  @throws(classOf[NoNodeException])
  def getDataExample(zkClient: ZkClient, path : String) : Array[Byte] = {
    //read data(pass)
    val data = zkClient(path).getData()
    val res = Await.result(data)
    //val resStr = new String(res.bytes)
    //println("data: " + resStr)
    res.bytes

  }

  @throws(classOf[NoNodeException])
  def setDataExample(zkClient: ZkClient, path : String, data : Array[Byte]) : Unit = {
    //val buf = ByteBuffer.wrap(new Array[Byte](data.length))
    //buf.put(data.getBytes)
    //each time need to set up different versions
    val version = getDataVersionExample(zkClient, path)
    //val write = zkClient(path).setData(buf.array(), version)
    val write = zkClient(path).setData(data, version)
    val res = Await.result(write)
  }

  def getDataVersionExample(zkClient: ZkClient, path : String) : Int = {
    val exists = zkClient(path).exists()
    val res = Await.result(exists)
    res.stat.getVersion
  }

  def checkPathExistExample(zkClient: ZkClient, path : String) : Boolean = {
    val exists = zkClient(path).exists()
    try {
      Await.result(exists)
      true
    }catch {
      case nne : NoNodeException => false
    }
  }


  //Single Layer Children(in Hurricane task_id is parent, and storage_node_id is child)
  def getChidrenExample(zkClient : ZkClient, path : String ) : Unit = {

   val children = zkClient(path).getChildren()
   val res = Await.result(children)
   val childrenList = res.children
   for (child <- childrenList){
     //get znode name
     //println("child path name: " + child.path)

     //get znode data
     //println("child data:" + getDataExample(zkClient, child.path))

     //delete
     deletePathExample(zkClient, child.path)
   }
    deletePathExample(zkClient,path)

  }

}

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


object zookeeper {
  def main(args: Array[String]): Unit = {

    //connections
    implicit val timer = new JavaTimer(true)
    val zkClient = ZkClient("localhost:2181", Some(5.seconds), 30.seconds)

    val path = "/testzookeeper"
    var version = -1

    //create path
    createPathExample(zkClient, path, 10)
    version = getDataVersionExample(zkClient, path)

    //set data to a existing path
    setDataExample(zkClient, path, "first test")

    //read data from a existing path
    getDataExample(zkClient, path)


    //delete path
    deletePathExample(zkClient, path)


  }

  @throws(classOf[NodeExistsException])
  def createPathExample(zkClient: ZkClient, path : String, data : Int) : Unit = {
    val acl: Seq[ACL] = OPEN_ACL_UNSAFE.asScala
    val buf = ByteBuffer.wrap(new Array[Byte](4))
    buf.putInt(data)
    val mode = zkClient.mode

    val create = zkClient(path).create(buf.array(), acl, mode)
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
  def getDataExample(zkClient: ZkClient, path : String) : Unit = {
    //read data(pass)
    val data = zkClient(path).getData()
    val res = Await.result(data)
    val resStr = new String(res.bytes)
    println("data: " + resStr)

  }

  @throws(classOf[NoNodeException])
  def setDataExample(zkClient: ZkClient, path : String, data : String) : Unit = {
    val buf = ByteBuffer.wrap(new Array[Byte](data.length))
    buf.put(data.getBytes)
    //each time need to set up different versions
    val version = getDataVersionExample(zkClient, path)
    val write = zkClient(path).setData(buf.array(), version)
    val res = Await.result(write)
  }

  def getDataVersionExample(zkClient: ZkClient, path : String) : Int = {
    val exists = zkClient(path).exists()
    val res = Await.result(exists)
    res.stat.getVersion
  }


  //Single Layer Children(in Hurricane task_id is parent, and storage_node_id is child)
  def getChidrenExample(zkClient : ZkClient, path : String ) : Unit = {

   val children = zkClient("/abc").getChildren()
   val res = Await.result(children)
   val childrenList = res.children
   for (child <- childrenList){
     //get znode name
     //println("child path name: " + child.path)

     //get znode data
     //println("child data:" + getDataExample(zkClient, child.path))
   }

  }

}

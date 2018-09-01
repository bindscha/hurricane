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
import scala.collection.mutable.ListBuffer


object ZK {
  def main(args: Array[String]): Unit = {

    //connections
    implicit val timer = new JavaTimer(true)
    val zkClient = ZkClient("localhost:2181", Some(5.seconds), 30.seconds)

    //TODO REMOVE for debug
    //val port1 = "/l27.0.0.1:2551"
    //val port2 = "/127.0.0.1:2552"
    //val port3 = "/127.0.0.1:2553"

    //val path = "/a4b36c3b-549e-3be3-b75d-7708582e54c9~C:\\\\Users\\\\j84091920\\\\Desktop\\\\input" + port2

    //val childrenPath = "/13adb6eb-33db-4504-8a09-e421fc0567da~input/127.0.0.1:2554"
    //val newPath = "/24adb6ee-33db-4504-8a09-e421fc0567da~input/127.0.0.1:2554"
    //var version = -1

    //val buf = ByteBuffer.wrap(new Array[Byte](24))
    //buf.putLong(0, 20)
    //buf.putLong(8, 4543)
    //buf.putLong(16, 255)

    //create path
    //createPathExample(zkClient, "/worker1_bag1", buf.array)
    //createPathExample(zkClient, "/worker2_bag1", buf.array)
    //createPathExample(zkClient, "/worker2_bag1/node2", buf.array())
    //createPathExample(zkClient, "/worker2_bag2", buf.array)
    //createPathExample(zkClient, "/worker2_bag2/node1", buf.array)
    //createPathExample(zkClient, "/worker2_bag2/node2", buf.array)


    //val test = getReference(zkClient, "/", "worker2", "worker3", "node2")
    //println("test:" + test)
    //testHelp2(zkClient, test, "worker2", "worker3")

    //val bytes = getDataExample(zkClient, "/worker3_bag1/node2")
    //printOffset(bytes)



    //version = getDataVersionExample(zkClient, path)

    //set data to a existing path
    //setDataExample(zkClient, path, buf.array())

    //read data from a existing path
    //println("path: " + path)
    //val bytes = getDataExample(zkClient, path)
    //printOffset(bytes)


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

    //deletePathExample(zkClient,"/replay/27539f30-c87c-437d-ace9-b0b11180511b~data")
    //deleteReplay(zkClient)
    //deletePathExample(zkClient, "/replay")
    //deleteAllPath(zkClient)
    //printAllOffset(zkClient)
    //printReplay(zkClient)
    //createPathExample(zkClient, "/replay", buf.array)


  }



  def getReference(zkClient: ZkClient, path : String, oldWorker : String, newWorker : String, address : String) : Map[String, String] ={
    var result : Map[String, String] = Map()
    val children = zkClient(path).getChildren()
    val res = Await.result(children)
    val childrenList = res.children
    for (child <- childrenList){
      if(child.path.contains(oldWorker)){
        val p = child.path + "/" + address
        if (checkPathExistExample(zkClient, p)){
          result += (p -> p.replace(oldWorker, newWorker))
        }
      }
    }
    result
  }

  def printOffset(bytes : Array[Byte], start : Int = 0) : Unit = {
    val buf = ByteBuffer.wrap(bytes)
    var i = start
    while(i < bytes.length){
      println("index at " + i + ": " + buf.getLong(i))
      i = i + 8
    }
  }


  def printReplay(zkClient: ZkClient): Unit = {
    val children = zkClient("/replay").getChildren()
    val res = Await.result(children)
    val childrenList = res.children
    for (child <- childrenList){
      if(!child.path.equalsIgnoreCase("/zookeeper") && !child.path.equalsIgnoreCase("/replay") ){
        println("path: " + child.path + "/127.0.0.1:2551")
        var bytes = getDataExample(zkClient, child.path + "/127.0.0.1:2551")
        printOffset(bytes, 0)

        //println("path: " + child.path + "/127.0.0.1:2552")
        //bytes = getDataExample(zkClient, child.path + "/127.0.0.1:2552")
        //printOffset(bytes, 0)

        //println("path: " + child.path + "/127.0.0.1:2553")
        //bytes = getDataExample(zkClient, child.path + "/127.0.0.1:2553")
        //printOffset(bytes, 0)


      }
    }
  }

  def printAllOffset(zkClient: ZkClient): Unit ={
    val children = zkClient("/").getChildren()
    val res = Await.result(children)
    val childrenList = res.children
    for (child <- childrenList){
      if(!child.path.equalsIgnoreCase("/zookeeper") && !child.path.equalsIgnoreCase("/replay") ){
        println("path: " + child.path + "/127.0.0.1:2551")
        var bytes = getDataExample(zkClient, child.path + "/127.0.0.1:2551")
        printOffset(bytes, 0)

        //println("path: " + child.path + "/127.0.0.1:2552")
        //bytes = getDataExample(zkClient, child.path + "/127.0.0.1:2552")
        //printOffset(bytes, 0)

        //println("path: " + child.path + "/127.0.0.1:2553")
        //bytes = getDataExample(zkClient, child.path + "/127.0.0.1:2553")
        //printOffset(bytes, 0)


      }
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



  def deleteAllPath(zkClient: ZkClient): Unit ={
    val children = zkClient("/").getChildren()
    val res = Await.result(children)
    val childrenList = res.children
    for (child <- childrenList){
      if(!child.path.equalsIgnoreCase("/zookeeper")){
        deletePathExample(zkClient, child.path + "/127.0.0.1:2551")
        //deletePathExample(zkClient, child.path + "/127.0.0.1:2552")
        //deletePathExample(zkClient, child.path + "/127.0.0.1:2553")
        deletePathExample(zkClient, child.path)
      }
    }
  }

  def deleteReplay(zkClient: ZkClient): Unit ={
    val children = zkClient("/replay").getChildren()
    val res = Await.result(children)
    val childrenList = res.children
    for (child <- childrenList){
      if(!child.path.equalsIgnoreCase("/zookeeper")){
        deletePathExample(zkClient, child.path + "/127.0.0.1:2551")
        //deletePathExample(zkClient, child.path + "/127.0.0.1:2552")
        //deletePathExample(zkClient, child.path + "/127.0.0.1:2553")
        deletePathExample(zkClient, child.path)
      }
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

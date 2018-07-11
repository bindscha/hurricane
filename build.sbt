import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

val akkaVersion = "2.5.12"

val project = Project(
  id = "hurricane",
  base = file(".")
)
  .settings(SbtMultiJvm.multiJvmSettings: _*)
  .settings(
    name := """hurricane""",
    version := "0.1.0",
    scalaVersion := "2.12.6",
    scalacOptions in Compile ++= Seq("-encoding", "UTF-8", "-target:jvm-1.8", "-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    javacOptions in Compile ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked", "-Xlint:deprecation"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-agent" % akkaVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,
      "com.twitter" %% "util-zk" % "18.6.0",
      "com.twitter" %% "util-collection" % "18.6.0",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "io.kamon" % "sigar-loader" % "1.6.6-rev002",
      "com.github.pathikrit" %% "better-files-akka" % "3.5.0",
      "io.jvm.uuid" %% "scala-uuid" % "0.2.4",
      "org.rogach" %% "scallop" % "3.1.2",
      "net.smacke" % "jaydio" % "0.1",
      "com.thedeanda" % "lorem" % "2.1",
      "it.unimi.dsi" % "dsiutils" % "2.3.6",
      "org.apache.commons" % "commons-math3" % "3.6.1" exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.hadoop" % "hadoop-client" % "3.1.0" exclude("org.slf4j", "slf4j-log4j12")  ,
      "net.agkn" % "hll" % "1.6.0",
      "com.google.guava" % "guava" % "21.0"),
    javaOptions in run ++= Seq(
      "-Xms8g", "-Xmx8g", "-XX:+UseG1GC", "-Daeron.term.buffer.length=67108864"),
    Keys.fork in run := true,
    mainClass in(Compile, run) := Some("ch.epfl.labos.hurricane.HurricaneApp")
  )

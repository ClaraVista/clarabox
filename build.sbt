<<<<<<< HEAD
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

=======
>>>>>>> 0153dac2ce6a1a68c4776ae79335bb381e7aca62
name := "clarabox"

version := "0.1"

scalaVersion := "2.9.3"
<<<<<<< HEAD

seq(assemblySettings: _*)

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "0.8.0-incubating",
    "org.apache.hbase" % "hbase" % "0.94.12",
    "junit" % "junit" % "4.10" % "test",
    "org.slf4j" % "slf4j-api" % "1.7.2",
    "org.slf4j" % "slf4j-log4j12" % "1.7.2"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
        case "META-INF/MANIFEST.MF" => MergeStrategy.discard
        case _ => MergeStrategy.first
    }
}
=======
>>>>>>> 0153dac2ce6a1a68c4776ae79335bb381e7aca62

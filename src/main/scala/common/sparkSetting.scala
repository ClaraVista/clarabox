package common

import scala.io.Source
import org.apache.spark.SparkContext
import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 11/5/13
 * Time: 1:44 PM
 */

object sparkSetting {

  // spark settings
  val nameNodeURL = Source.fromFile("/root/spark-ec2/masters").mkString.trim
  val hostname = Source.fromFile("/etc/hostname").mkString.trim
  val sparkPort = 7077
  val hdfsPort = 9010

  val dependencies = new File("lib/").listFiles().map(_.getName).toList.filter(!_.contains("spark"))

  val sparkContext = {

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "common.MyRegistrator")
//    System.setProperty("spark.kryoserializer.buffer.mb", "50")
//    System.setProperty("spark.executor.memory", "10g")
//    System.setProperty("spark.scheduler.mode", "FAIR")
//    System.setProperty("spark.akka.frameSize", "50")

    val sc = new SparkContext("spark://" + nameNodeURL + ":" + sparkPort, "HBaseTest",
      System.getenv("SPARK_HOME"), Seq("target/scala-2.9.3/clarabox_2.9.3-0.1.jar"))

    val awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    val awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", awsAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

    // add all dependencies
    dependencies.foreach(jar => sc.addJar("lib/" + jar))
    sc
  }
}

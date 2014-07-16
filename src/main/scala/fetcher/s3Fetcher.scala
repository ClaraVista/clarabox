package fetcher

import common.sparkSetting._
import org.apache.spark.rdd.RDD

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 1/8/14
 * Time: 4:02 PM
 */
object s3Fetcher {
  val s3BucketName = ""
  val basePath = "s3n://" + s3BucketName

  def readData(path: String) = {
    sparkContext.textFile(basePath + path)
  }

  def writeData[T](dataSet: RDD[T], path: String) = {
    dataSet.saveAsTextFile(basePath + path)
  }
}

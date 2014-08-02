package fetcher

import common.sparkSetting._
import org.apache.spark.rdd.RDD

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/3/13
 * Time: 3:33 PM
 */

object hdfsFetcher {

  // val basePath = "hdfs://" + nameNodeURL + ":" + hdfsPort
  val basePath = "hdfs://" + "" + ":" + ""

  def readData(path: String) = {
    sparkContext.textFile(basePath + path)
  }

  def writeData[T](dataSet: RDD[T], path: String) = {
    dataSet.saveAsTextFile(basePath + path)
  }
}

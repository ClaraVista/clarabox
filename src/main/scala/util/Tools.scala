package util

import org.apache.spark.rdd.RDD

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 1/30/14
 * Time: 10:35 AM
 */
object Tools {

  def concatener (param : List[List[Any]]): List[Any] = {
    param match {
      case Nil => Nil
      case x :: xs => x :: concatener(xs)
    }
  }

}


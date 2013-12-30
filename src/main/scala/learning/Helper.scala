package learning

import org.apache.spark.rdd.RDD

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/30/13
 * Time: 3:09 PM
 *
 * This object contains all context-free helper functions used in machine learning process
 */

object Helper {
  def split[T: ClassManifest](data: RDD[T], p: Double, seed: Long = System.currentTimeMillis): (RDD[T], RDD[T]) = {
    val rand = new scala.util.Random(seed)
    val partitionSeeds = data.partitions.map(partition => rand.nextLong)
    val temp = data.mapPartitionsWithIndex((index, iter) => {
      val partitionRand = new java.util.Random(partitionSeeds(index))
      iter.map(x => (x, partitionRand.nextDouble))
    })
    (temp.filter(_._2 <= p).map(_._1), temp.filter(_._2 > p).map(_._1))
  }
}

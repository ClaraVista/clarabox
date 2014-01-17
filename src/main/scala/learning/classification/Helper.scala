package learning.classification

import org.apache.spark.rdd._
import learning.classification.output.{Summary, LabeledScore}


/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/30/13
 * Time: 3:09 PM
 *
 * This object contains all context-free helper functions used in machine learning process
 */

object Helper {

  /**
   *
   * @param data input data set
   * @param p partition percentage
   * @param seed a random seed for generating random partition
   * @tparam T type parameter for input RDD
   * @return two partitions where the first contains p % elements, and the second takes (1 - p) % elements
   */
  def split[T: ClassManifest](data: RDD[T], p: Double, seed: Long = System.currentTimeMillis): (RDD[T], RDD[T]) = {
    val rand = new scala.util.Random(seed)
    val partitionSeeds = data.partitions.map(partition => rand.nextLong)
    val temp = data.mapPartitionsWithIndex((index, iter) => {
      val partitionRand = new scala.util.Random(partitionSeeds(index))
      iter.map(x => (x, partitionRand.nextDouble))
    })
    (temp.filter(_._2 <= p).map(_._1), temp.filter(_._2 > p).map(_._1))
  }

  /**
   *
   * @param scoreSet a Score set used for measure binary classification error
   * @return  a list of error according to the corresponding label
   */
  def errorMeasure(scoreSet: RDD[Summary], nbMod: Int = 1) = {
    (0 until nbMod) map {
      index => scoreSet.filter(scr => scr.scoreList(index).isError).count.toDouble / scoreSet.count
    }
  }

  /**
   *
   * @param pairList a label-score pair list
   * @param percentage the quantile of the total population
   * @return a list of coordinate on the ROC
   */
  def generateLiftCurveData(pairList: List[LabeledScore], percentage: Double) = {

    val nbSplit = 1 / percentage
    val nbTotal = pairList.size
    val nbOnes = pairList.count(_.label == 1)
    val share = (nbTotal / nbSplit).toInt

    def getPartition(lst: List[LabeledScore]): List[Double] = {
      if (lst.size < share) List(lst.count(_.label == 1))
      else lst.take(share).count(_.label == 1) :: getPartition(lst.drop(share))
    }

    val dist = getPartition(pairList.sortBy(-_.score))

    val tail = 1 to dist.size map {
      i => if (i == dist.size) (1.0, dist.sum / nbOnes) else (share * i / nbTotal.toDouble, dist.take(i).sum / nbOnes)
    }

    (0.0, 0.0) +: tail
  }

  //  def orderedSplit(dataSet: RDD[(Double, Double)], nb: Int): Array[Double] = {
  //    dataSet.partitions
  //    val share: Int = (dataSet.count / nb).toInt
  //    val localSet = dataSet.collect.take(share)
  //  }
}

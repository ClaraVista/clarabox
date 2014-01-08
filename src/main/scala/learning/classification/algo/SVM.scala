package learning.classification.algo

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.rdd.RDD
import learning.classification.input.Individual

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/24/13
 * Time: 10:48 AM
 */

class SVM(val scoringDataSet: RDD[Individual],
          val numIterations: Int,
          val nbMod: Int = 1)
  extends BasicClassification with Serializable {
  require(scoringDataSet.first.labels.size == nbMod)
  val models = trainData(new SVMWithSGD())
}
package learning.classification.algo

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.rdd.RDD
import learning.classification.input.Individual


/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/24/13
 * Time: 10:46 AM
 */
class LogReg(val scoringDataSet: RDD[Individual],
             val numIterations: Int,
             val nbMod: Int = 1)
  extends BasicClassification with Serializable {
  val models = trainData(new LogisticRegressionWithSGD())
}

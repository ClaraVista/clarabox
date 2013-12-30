package learning.classification

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD


/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/24/13
 * Time: 10:46 AM
 */
class LogReg(val scoringDataSet: BasicClassification#ScoringDataSet, val numIterations: Int) extends BasicClassification with Serializable {
  val model =  trainData(new LogisticRegressionWithSGD())
}

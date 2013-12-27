package learning.classification

import org.apache.spark.mllib.classification.SVMWithSGD

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/24/13
 * Time: 10:48 AM
 */

class SVM(val scoringDataSet: BasicClassification#ScoringDataSet) extends BasicClassification with Serializable {
  val numIterations: Int = 10
  val model =  trainData(new SVMWithSGD())
}
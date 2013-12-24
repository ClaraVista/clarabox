package learning.classification

import org.apache.spark.mllib.classification.SVMWithSGD

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/24/13
 * Time: 10:48 AM
 */
trait SVM extends BasicClassification{
  override lazy val model = getModel(new SVMWithSGD())
}

package learning.classification

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/24/13
 * Time: 10:46 AM
 */
trait LogReg extends BasicClassification{
    override lazy val model = getModel(new LogisticRegressionWithSGD())
}

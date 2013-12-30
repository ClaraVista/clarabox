package learning.classification

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, GeneralizedLinearAlgorithm}
import org.apache.spark.mllib.optimization.{GradientDescent, L1Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import learning.Helper._
import scala.math.exp

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/23/13
 * Time: 11:51 AM
 *
 */

trait BasicClassification {
  type DataSet = RDD[LabeledPoint]
  type ScoringDataSet = RDD[(Int, LabeledPoint)]

  /**
   * Variables to be implemented when mix-in
   */
  val scoringDataSet: ScoringDataSet
  val numIterations: Int
  val model: GeneralizedLinearModel

  val dataSet: RDD[LabeledPoint] = scoringDataSet map {
    case (id, labeledPoint) => labeledPoint
  }

  val (trainingSet, validationSet, testSet) = {
    val (a, remainder) = split(dataSet, 0.6)
    val (b, c) = split(remainder, 0.5)
    (a, b, c)
  }

  lazy val E_in = errorMeasure(trainingSet)
  lazy val E_out = errorMeasure(testSet)
  lazy val scores = {
    // logistic function
    def logistic(x: Double) = 1 / (1 + exp(-x))

    scoringDataSet.map {
      case (id, pt) =>
        //        require(model.weights.size == pt.features.size)
        val x = (model.weights zip pt.features).map(p => p._1 * p._2).sum + model.intercept
        val prob: Double = logistic(x)
        (id, model.predict(pt.features), prob)
    }
  }

  /**
   * @param algorithm SVM, logistic regression, and other classification algorithms.
   * @tparam T the type of the model which is a subclass of GeneralizedLinearModel
   * @return the correspond model
   */
  protected def trainData[T <: GeneralizedLinearModel](algorithm: GeneralizedLinearAlgorithm[T]): T = {
    val optimizer = algorithm.optimizer match {
      case opt: GradientDescent => opt
    }
    optimizer.setNumIterations(numIterations)
      .setRegParam(0.1)
      .setUpdater(new L1Updater)

    algorithm.run(trainingSet)
  }

  /**
   * @param ds the dataSet to estimate with
   * @return binary classification error
   */
  protected def errorMeasure(ds: RDD[LabeledPoint]) = {
    val res = ds.map {
      point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
    }
    res.filter(r => r._1 != r._2).count.toDouble / res.count
  }
}
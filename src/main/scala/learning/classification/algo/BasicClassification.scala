package learning.classification.algo

import learning.classification.Helper
import learning.classification.output.{Summary, LabeledScore}
import learning.classification.input.Individual
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, GeneralizedLinearAlgorithm}
import org.apache.spark.mllib.optimization.{GradientDescent, L1Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import scala.math.log

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/23/13
 * Time: 11:51 AM
 *
 * A basic trait used for classification, multi-modality supported.
 * Note: the in/out-of sample error is based on Score, may be changed afterwards.
 */

trait BasicClassification {

  /*
   *learning parameters
   */


  // partition percentage of the total data set
  val trainingSetPercentage = 0.6
  val testSetPercentage = 0.2
  val validationSetPercentage = 1 - trainingSetPercentage - testSetPercentage

  // two target labels
  val labelOne = 1.0
  val labelZero = 0.0

  /*
   * Variables to be implemented for mix-in
   */

  // defined in subclass' parameter
  val nbMod: Int
  val numIterations: Int
  val scoringDataSet: RDD[Individual]

  // defined in the subclass
  val models: List[GeneralizedLinearModel]

  /*
   * Pre-defined variables
   */

  // define the three sets used by learning according to the percentage
  val (trainingSet, validationSet, testSet) = {
    val (a, remainder) = Helper.split(scoringDataSet, trainingSetPercentage)
    val (b, c) = Helper.split(remainder, validationSetPercentage / (1 - trainingSetPercentage))
    //    (a, b, c)
    (a union b, Nil, c)
  }

  /*
   * adjustment added on intercept if multi-modality, 0 if mono-modality
   * in order to removing the influence of imbalanced sampling (make score comparable)
   */
  val adjList = 0 until nbMod map {
    i => if (nbMod == 1) 0.0 else adjustment(i)
  }

  lazy val trainingSummary = scoring(trainingSet)
  lazy val testSummary = scoring(testSet)

  lazy val E_in = Helper.errorMeasure(trainingSummary, nbMod)
  lazy val E_out = Helper.errorMeasure(testSummary, nbMod)

  /**
   *
   * @param dataSet an Individual set whose features are used to calculate score
   * @return a set containing id, list of label-score pair
   */
  protected def scoring(dataSet: RDD[Individual]) = {
    dataSet.map {
      indiv =>
        val scoreList = (models zip adjList) map {
          case (model, adj) => featureScoring(indiv.features, adj, model)
        }
        Summary(indiv.id,
          (indiv.labels zip scoreList).map {
            case (label, score) => LabeledScore(label, score)
          }.toList)
    }
  }

  /**
   *
   * @param features features of a data point
   * @param adjustment regularization according to the percentage
   * @param mdl a GLM calculated from LR or SVM
   * @return a probability based on logistic function (even if the algo is SVM)
   */
  protected def featureScoring(features: Array[Double], adjustment: Double, mdl: GeneralizedLinearModel) = {
    require(mdl.weights.size == features.size)
    (mdl.weights zip features).map(p => p._1 * p._2).sum + mdl.intercept + adjustment
  }

  /**
   *
   * @param index the index of label
   * @return the adjustment on the intercept
   */
  protected def adjustment(index: Int) = {
    val distTotal = scoringDataSet.map {
      indiv => indiv.labels(index)
    }.countByValue

    val distSample = trainingSet.map {
      indiv => indiv.labels(index)
    }.countByValue

    log(distSample(labelOne).toDouble / distSample(labelZero).toDouble) - log(distTotal(labelOne).toDouble / distTotal(labelZero).toDouble)
  }

  /**
   *
   * @param algorithm SVM, logistic regression, and other classification algorithms.
   * @tparam T the type of the model which is a subclass of GeneralizedLinearModel
   * @return the correspond model
   */
  protected def trainData[T <: GeneralizedLinearModel](algorithm: GeneralizedLinearAlgorithm[T]): List[T] = {
    val regularizationParam: Double = 0.01

    val retVal = for (i <- 0 until nbMod) yield {
      val optimizer = algorithm.optimizer match {
        case opt: GradientDescent => opt
      }

      optimizer.setNumIterations(numIterations)
        .setRegParam(regularizationParam)
      //.setUpdater(new L1Updater)

      algorithm.run(trainingSet.map {
        indiv => LabeledPoint(indiv.labels(i), indiv.features)
      })
      //      SVMWithSGD.train(trainingSet.map {
      //        indiv => LabeledPoint(indiv.labels(i), indiv.features)
      //      }, numIterations)
    }
    retVal.toList
  }
}
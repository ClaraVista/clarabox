package learning.classification

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, GeneralizedLinearAlgorithm}
import org.apache.spark.mllib.optimization.{GradientDescent, L1Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import scala.math.exp

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/23/13
 * Time: 11:51 AM
 */

trait BasicClassification {
  type DataSet = RDD[LabeledPoint]
  type ScoringDataSet = RDD[(Int, LabeledPoint)]

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

  lazy val E_in = errorMeasure(trainingSet, model)
  lazy val E_out = errorMeasure(testSet, model)
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

  protected def trainData[T <: GeneralizedLinearModel](algorithm: GeneralizedLinearAlgorithm[T]) = {
    val optimizer = algorithm.optimizer match {
      case opt: GradientDescent => opt
    }
    optimizer.setNumIterations(numIterations)
      .setRegParam(0.1)
      .setUpdater(new L1Updater)

    algorithm.run(trainingSet)
  }

  protected def split[T: ClassManifest](data: RDD[T], p: Double, seed: Long = System.currentTimeMillis): (RDD[T], RDD[T]) = {
    val rand = new scala.util.Random(seed)
    val partitionSeeds = data.partitions.map(partition => rand.nextLong)
    val temp = data.mapPartitionsWithIndex((index, iter) => {
      val partitionRand = new java.util.Random(partitionSeeds(index))
      iter.map(x => (x, partitionRand.nextDouble))
    })
    (temp.filter(_._2 <= p).map(_._1), temp.filter(_._2 > p).map(_._1))
  }

  protected def errorMeasure(ds: RDD[LabeledPoint], model: GeneralizedLinearModel) = {
    val res = ds.map {
      point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
    }
    res.filter(r => r._1 != r._2).count.toDouble / res.count
  }
}
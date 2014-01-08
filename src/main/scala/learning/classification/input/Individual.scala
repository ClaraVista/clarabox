package learning.classification.input

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 1/6/14
 * Time: 3:51 PM
 */


case class Individual(id: Int, labels: Array[Double], features: Array[Double]) {
  override def toString = id + ";" + labels.toList + ";" + features.toList
}
package job.analysis

import org.apache.spark.mllib.regression.LabeledPoint
import fetcher.hdfsFetcher
import learning.classification.LogReg

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/23/13
 * Time: 12:00 PM
 */
object anonymous extends LogReg {

  val dataLocation = "/ML/rl_clean"

  val numIterations = 200
  val scoringDataSet: ScoringDataSet = hdfsFetcher.readData(dataLocation).map {
    line =>
      val parts = line.split("\001")
      // first element is ID which is not a variable
      (parts.head.toInt, LabeledPoint(parts.tail.head.toDouble, parts.tail.tail.map(x => x.toDouble).toArray))
  }

  val dataSet = scoringDataSet map {
    case (id, labeledPoint) => labeledPoint
  }

  def run() = {
//        println
//        println(dataSet.map(pt => pt.features.toList) take 5 mkString "\n")
//        println

    println
    println("=== Logistic Regression ===")
    println("In-sample error = " + E_in)
    println("Out-of-sample error = " + E_out)
    println
  }

}

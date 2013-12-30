package job.analysis.anonymous

import org.apache.spark.mllib.regression.LabeledPoint
import fetcher.hdfsFetcher
import learning.classification.LogReg
import org.apache.spark.SparkContext._

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/23/13
 * Time: 12:00 PM
 */
object launcher {
  val dataLocation = "/ML/rl_clean"
  val resultLocation = "/res/anonymous_to_delete/"

  def saveScores() = {
    val scoringDataSet = hdfsFetcher.readData(dataLocation).map {
      line =>
        val parts = line.split("\001")
        // first element is ID which is not a variable
        (parts.head.toInt, LabeledPoint(parts.tail.head.toDouble, parts.tail.tail.map(x => x.toDouble).toArray))
    }

    val cls = new LogReg(scoringDataSet, 100)
    hdfsFetcher.writeData(cls.scores.map(p => p._1 + ";" + p._2 + ";" + p._3), resultLocation)
  }

  def loadScores() = {
    val sco = hdfsFetcher.readData(resultLocation).map(line => line split ";") map {
      case Array(id, label, prob) => (prob.toDouble, label.toDouble)
    }
    sco.filter(x => x._2 == 1).sortByKey(ascending = false) take 100 mkString "\n"
  }

  def run() = {
    saveScores
    //    loadScores
  }
}

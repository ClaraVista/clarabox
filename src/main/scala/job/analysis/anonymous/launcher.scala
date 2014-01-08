package job.analysis.anonymous

import fetcher.hdfsFetcher
import learning.classification._
import learning.classification.algo.LogReg
import learning.classification.output.Summary
import learning.classification.input.Individual

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/23/13
 * Time: 12:00 PM
 */

object launcher {
  val monoModLocation = "/ML/rl_clean"
  val multiModLocation = "/ML/rl_mm"
  val monoModResultLocation = "/res/anonymous/"
  val multiModResultLocation = "/res/mmAnonymous/"

  /*
   * mono-modality
   */

  def saveScores() = {
    val scoringDataSet = hdfsFetcher.readData(monoModLocation).map {
      line =>
        val parts = line.split("\001")
        // first element is ID which is not a variable
        Individual(parts.head.toInt, Array(parts.tail.head.toDouble), parts.tail.tail.map(x => x.toDouble).toArray)
    }

    val cls = new LogReg(scoringDataSet, numIterations = 200)
    hdfsFetcher.writeData(cls.testSummary.map(_.toString()), monoModResultLocation)
  }

  def loadScores() = {
    val sco = hdfsFetcher.readData(monoModResultLocation).map(line => (line split ";").toList).map(Summary.apply).map(_.scoreList.head)
    Helper.generateLiftCurveData(sco.collect.toList, 0.05).mkString("\n")
  }

  /*
   * multi-modality
   */

  def saveMMScores() = {
    val nbModality: Int = 3

    val scoringDataSet = hdfsFetcher.readData(multiModLocation).map {
      line =>
        val parts = line.split("\001")
        Individual(parts.head.toInt, parts.tail.take(nbModality).map(_.toDouble).toArray, parts.tail.drop(nbModality).map(_.toDouble).toArray)
    }

    val cls = new LogReg(scoringDataSet, numIterations = 100, nbModality)

    hdfsFetcher.writeData(cls.testSummary.map(_.toString), multiModResultLocation)
  }

  def loadMMScores() = {
    val content = hdfsFetcher.readData(multiModResultLocation).map(line => (line split ";").toList).map(Summary.apply)
    content take 100 mkString "\n"
  }

  def run() = {
    //    saveScores
    println(loadScores)

    //    saveMMScores()
    //    println(loadMMScores)
  }
}

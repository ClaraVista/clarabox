package job.analysis.anonymous

import common.sparkSetting._
import fetcher.hdfsFetcher
import fetcher.s3Fetcher
import learning.classification._
import learning.classification.algo.{SVM, LogReg}
import learning.classification.output.Summary
import learning.classification.input.Individual

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/23/13
 * Time: 12:00 PM
 */

object launcher {
  //  val monoModLocation = "/ML/rl_clean"
  //  val monoModResultLocation = "/res/anonymous/"

  //  val monoModLocation = "/ML/rl_bal"
  //  val monoModResultLocation = "/res/anonymous_bal/"

  val monoModLocation = "/ML/rl_p2"
  //  val monoModResultLocation = "/res/anonymous_p2/"
  val monoModResultLocation = "/res/digit/"

  val multiModLocation = "/ML/rl_mm"
  val multiModResultLocation = "/res/mmAnonymous/"

  /*
   * mono-modality
   */

  def saveScores() = {
    //    val scoringDataSet = hdfsFetcher.readData(monoModLocation).map {
    //      line =>
    //        val parts = line.split("\001")
    //        // first element is ID which is not a variable
    //        Individual(parts.head.toInt, Array(parts.tail.head.toDouble), parts.tail.tail.map(x => x.toDouble).toArray)
    //    }

    val trainingSet = hdfsFetcher.readData("/svm/features.train.txt").map {
      line => line.replaceAll("^\\s+", "").split("\\s+")
    }
//      .filter(line => line.head.toDouble == 1.0 || line.head.toDouble == 5.0)
      .map {
      line => if (line.head.toDouble == 1.0) Individual(1, Array(1.0), line.tail.map(x => x.toDouble).toArray)
      else Individual(1, Array(0.0), line.tail.map(x => x.toDouble).toArray)
    }

    val testSet = hdfsFetcher.readData("/svm/features.test.txt").map {
      line => line.replaceAll("^\\s+", "").split("\\s+")
    }
//      .filter(line => line.head.toDouble == 1.0 || line.head.toDouble == 5.0)
      .map {
      line => if (line.head.toDouble == 1.0) Individual(1, Array(1.0), line.tail.map(x => x.toDouble).toArray)
      else Individual(1, Array(0.0), line.tail.map(x => x.toDouble).toArray)
    }

    val scoringDataSet = trainingSet union testSet

    val cls = new SVM(scoringDataSet, numIterations = 200)

    val onePerc = scoringDataSet.filter(x => x.labels.head == 1.0).count / scoringDataSet.count.toDouble
    val nbTotal = scoringDataSet.count
    val Ein = cls.E_in
    val Eout = cls.E_out


    hdfsFetcher.writeData(cls.testSummary.map(_.toString()), monoModResultLocation)
    val sco = hdfsFetcher.readData(monoModResultLocation).map(line => (line split ";").toList).map(Summary.apply) //.map(_.scoreList.head)

    println("one 's percentage = " + onePerc)
    println("nbTotal = " + nbTotal)
    println("E_in = " + Ein)
    println("E_out = " + Eout)
  }

  //  def loadScores() = {
  //    val sco = hdfsFetcher.readData(monoModResultLocation).map(line => (line split ";").toList).map(Summary.apply) //.map(_.scoreList.head)
  //    Helper.errorMeasure(sco)
  //  }

  def lift() = {
    val path = "liftTest"
    val sco = hdfsFetcher.readData(monoModResultLocation).map(line => (line split ";").toList).map(Summary.apply).map(_.scoreList.head)
    val liftData = Helper.generateLiftCurveData(sco.collect.toList, 0.01)
    val liftRDD = sparkContext.parallelize(liftData).map {
      case (x, y) => x + ";" + y
    }
    s3Fetcher.writeData(liftRDD, path)
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
    saveScores
    //    println("in hdfs" + loadScores)
    lift

    //    saveMMScores()
    //    println(loadMMScores)
  }
}

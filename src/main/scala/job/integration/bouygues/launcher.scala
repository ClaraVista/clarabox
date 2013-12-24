package job.integration.bouygues

import fetcher.{hdfsFetcher, hbaseFetcher}
import job.integration.bouygues.entity.BygClient


/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 11/5/13
 * Time: 1:36 PM
 */

object launcher {

  // job settings
  val inputTable, outputTable = "byg_client_native"
  val noteFile = "/output/navi/notes"
  val mapFile = "/output/navi/pageCtgMap"

  // These members are not necessary, because maps are often pre-defined, no need to be build from the base
  def createMap() = {

    // get raw data result
    val hBaseRDD = hbaseFetcher.readData(inputTable)

    // deserialize data
    val data = hBaseRDD.map(BygClient(_).pageCtg).flatMap(x => x).map(x => x._1 + ";" + x._2).distinct()

    hdfsFetcher.writeData(data, mapFile)
  }

  def calculateNotes() = {

    // get raw data result
    val hBaseRDD = hbaseFetcher.readData(inputTable)

    val params = BouyguesParams()
    val data = hBaseRDD.map(BygClient(_).retrieveNote(params).toString)
    println(data.count)
  }

  //  def readNotes() = {
  //    val test = sparkContext.textFile("hdfs://" + nameNodeURL + ":" + hdfsPort + "/output/navi/note").map(_.split(";"))
  //    println(test.count)
  //  }

  def run() = {

    //    createMap
    calculateNotes
    //    readNotes()
    //    println(pageCtgMap.size)
  }
}

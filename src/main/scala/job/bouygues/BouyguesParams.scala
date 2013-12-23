package job.bouygues

import fetcher.hdfsFetcher
import job.bouygues.launcher._
import parameter.AnnouncerParams

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/13/13
 * Time: 3:11 PM
 */

case class BouyguesParams extends AnnouncerParams {

  // some parameters for announcer: Bouygues
  val pageCtgMap = hdfsFetcher.readData(mapFile).map(_.split(";")).map(x => (x(0), x(1).toInt)).collect.toMap

  // ...
}



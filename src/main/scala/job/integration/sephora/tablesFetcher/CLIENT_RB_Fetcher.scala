package job.integration.sephora.tablesFetcher

import fetcher.csvFetcher

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 8/2/14
 * Time: 8:17 PM
 */
object CLIENT_RB_Fetcher{

  case class idRb_idClient(idRb: String, idClient: String)

  def generate_table()={
    val r = csvFetcher.readData("/home/spark/workspace/IdeaProject/clarabox/data/Sephora/Client_RB_anonymes0.csv",
                                has_header = true, delimiter = ",", string_delimiter = "\"")

    r.map(t => idRb_idClient(t(0), t(1)))
  }
}

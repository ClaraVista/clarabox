package job.integration.sephora.tablesFetcher

import fetcher.csvFetcher

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 8/2/14
 * Time: 10:31 PM
 */
object FAMILLES_OLFACTIVES_Fetcher{

  case class Famille_Olfactive(Material_Description: String, Material_ID: String, Sub_Brand_Code: String, FAMILLE: String, SOUS_FAMILLE: String)

  def generate_table()={
    val r = csvFetcher.readData("/home/spark/workspace/IdeaProject/clarabox/data/Sephora/Familles_olfactives.csv", has_header = true, delimiter = ",", string_delimiter = "\"")

    r.map(t => Famille_Olfactive(t(0), t(1), t(2), t(3), t(4)))
  }
}

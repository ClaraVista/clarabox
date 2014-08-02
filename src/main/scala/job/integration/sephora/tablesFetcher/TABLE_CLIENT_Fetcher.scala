package job.integration.sephora.tablesFetcher

import fetcher.csvFetcher
import org.joda.time.DateTime
import job.integration.sephora.tools.StringToDate

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 8/2/14
 * Time: 8:22 PM
 */
object TABLE_CLIENT_Fetcher{

  case class Client(current_Card_Status_ID: String, rfm_Segment_ID: String ,gender_ID: String,
                    age: Int, subscription_Date: DateTime, nouvel_id_client: String)

  def generate_table()={
    val r = csvFetcher.readData("/home/spark/workspace/IdeaProject/clarabox/data/Sephora/Clts-anonymes0.csv",
                                has_header = true, delimiter = ",", string_delimiter = "\"")

    r.map(t => Client(t(0), t(1), t(2), t(3).toInt, StringToDate.from_format_table_client_to_datetime(t(4)), t(5)))
      .filter(_.subscription_Date != null)
  }
}

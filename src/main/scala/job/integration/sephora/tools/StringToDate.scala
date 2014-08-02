package job.integration.sephora.tools

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.DateTime
import scala.util.matching.Regex

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 8/1/14
 * Time: 11:05 PM
 */
object StringToDate{


  val patternClaraVista: Regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}".r
  val patternClassic: Regex = "[0-9]{2}/[0-9]{2}/[0-9]{4}".r
  val patternTableClient: Regex = "[0-9]{8}".r

  val formatClaraVista: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  val formatClassic: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy")
  val formatTableClient: DateTimeFormatter = DateTimeFormat.forPattern("yyyymmdd")



  def from_format_classic_to_datetime(s: String): DateTime={
    formatClassic.parseDateTime(s)
  }

  def from_format_claravista_to_datetime(s: String): DateTime={
    formatClaraVista.parseDateTime(s)
  }

  def from_format_table_client_to_datetime(s: String): DateTime={
    if(patternTableClient.findAllMatchIn(s).size > 0){
     formatTableClient.parseDateTime(s)
    }else{
      null
    }
  }
}

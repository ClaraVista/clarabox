package job.integration.bouygues

import serde._
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.util.Vector
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.Days
import component.BasicClient
import component.browse.{BasicView, BasicVisit}
import scala.Some
import parameter.AnnouncerParams
import activity.Browse
import learning.IdentifiedFeatures

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 11/6/13
 * Time: 3:26 PM
 */

object entity {

  object View extends EntitySerde[View] {
    val fieldSep = "\005"
    val listElemSep: String = ""

    def serialize(ent: View) = {
      List(ent.page, ent.idCtg, ent.freq).mkString(fieldSep)
    }

    def deserialize(src: String) = {
      src.split(fieldSep) match {
        case Array(page, idCtg, freq) => View(page, idCtg.toInt, freq.toInt)
      }
    }
  }

  case class View(page: String, idCtg: Int, freq: Int) extends BasicView {
    override def toString = "\n\t\tpage: " + page + "\tidCtg: " + idCtg + "\tfreq: " + freq
  }

  object Visit extends EntitySerde[Visit] {
    val fieldSep = "\003"
    val listElemSep = "\004"

    def serialize(obj: Visit) = {
      List(obj.cookie, obj.date, obj.isVerified, obj.views.map(View.serialize).mkString(listElemSep)).mkString(fieldSep)
    }

    def deserialize(src: String) = {
      src.split(fieldSep) match {
        case Array(cookie, date, isVerified, views) => Visit(cookie, date, isVerified.toBoolean, views.split(listElemSep).map(View.deserialize).toList)
      }
    }
  }

  case class Visit(cookie: String, date: String, isVerified: Boolean, views: List[View]) extends BasicVisit {
    override def toString = "\n\tcookie: " + cookie + "\n\tdate: " + date + "\n\tisVerified: " + isVerified + "\n\tviews: " + views + "\n"
  }

  object BygClient {
    val visitItemSep = "\002"

    def apply(res: Result): BygClient = {
      val id = Bytes.toString(res.getRow)
      res.raw.map(kv => Bytes.toString(kv.getValue)) match {
        case Array(isFictive, isValid, visits) =>
          BygClient(id, isFictive.toBoolean, isValid.toBoolean, visits.split(visitItemSep).map(Visit.deserialize).toList)
      }
    }
  }

  case class BygClient(id: String, isFictive: Boolean, isValid: Boolean, visits: List[Visit])
    extends BasicClient with Browse with HBaseSerializable {

    //    override def toString = "id: " + id + "\nisFictive: " + isFictive + "\nisValid: " + isValid + "\nvisits: " + visits + "\n"

    val key = id.toString

    def dataBindings = List(
      // column-family / qualifier / value
      hbaseRowFormat("navi", "isfictif", isFictive),
      hbaseRowFormat("navi", "isvalid", isValid),
      hbaseRowFormat("navi", "visites", visits.map(Visit.serialize).mkString(BygClient.visitItemSep)) // serialize to String
    )

    override def retrieveNote(params: AnnouncerParams): IdentifiedFeatures = {

      val dict = params match {
        case p: BouyguesParams => p.pageCtgMap
        case _ => throw new Exception("Params is not matched with BouyguesParameters")
      }

      val ctgMap = visits.map(v => v.views.map(aff => (dict(aff.page), aff.freq)))
        .flatten.groupBy(_._1)
        .map(b => (b._1, b._2.map(_._2).sum))

      val freqList = 1 to 100 map (i => ctgMap.get(i) match {
        case Some(x) => x
        case None => 0d
      })

      IdentifiedFeatures(id, Vector(getHeat("20/09/2013", 4, 2, 1) +: freqList.toArray))
    }

    // Helper functions

    // This function is not necessary if we have already a map
    def pageCtg = {
      visits.map(v => v.views.map(aff => (aff.page, aff.idCtg))).flatten.distinct
    }

    def getHeat(currDay: String, days: Int, weeks: Int, months: Int): Double = {
      def dateDiff(strDate: String) = {
        val formatter: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy")
        Days.daysBetween(formatter.parseDateTime(strDate), formatter.parseDateTime(currDay)).getDays
      }
      val dayList = visits.map(v => dateDiff(v.date))
      val nbVstLastDays = dayList.count(d => d < days && d > 0)
      val nbVstLastWeeks = dayList.count(d => d < 7 * weeks && d > 0)
      val nbVstLastMonths = dayList.count(d => d < 30 * months && d > 0)
      (nbVstLastDays + 1) * (nbVstLastWeeks + 1) * (nbVstLastMonths + 1)
    }
  }
}


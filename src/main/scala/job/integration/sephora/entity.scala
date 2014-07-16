package job.integration.sephora

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import component.BasicClient
import serde.{EntitySerde, HBaseSerializable}
import component.purchase.{BasicTicket, BasicSale}


/**
 * Created with IntelliJ IDEA.
 * User: hadoop
 * Date: 12/19/13
 * Time: 5:42 PM
 */

object entity {

  object Sale extends EntitySerde[Sale] {
    val fieldSep = "\005"
    val listElemSep: String = ""

    def serialize(ent: Sale) = {
      List(ent.idProduct, ent.quantity, ent.price).mkString(fieldSep)
    }

    def deserialize(src: String) = {
      src.split(fieldSep) match {
        case Array(idProduct, quantity, price) => Sale(idProduct, quantity.toInt, price.toFloat)
      }
    }

  }

  case class Sale(idProduct: String, quantity: Int, price: Float) extends BasicSale {
    override def toString = "\n\t\tmaterial_id: " + idProduct + "\tquantity: " + quantity + "\tsales_VAT: " + price
  }



  object Ticket extends  EntitySerde[Ticket]{

    val listElemSep = "\004"
    val fieldSep = "\003"

    def serialize(obj: Ticket) = {
      List(obj.idTicket, obj.purchaseDate, obj.sales.map(Sale.serialize).mkString(listElemSep)).mkString(fieldSep)
    }

    def deserialize(src: String) = {
      src.split(fieldSep) match {
        case Array(idTicket, purchaseDate, sales) => Ticket(idTicket, purchaseDate, sales.split(listElemSep).map(Sale.deserialize).toList)
      }
    }

  }

  case class Ticket(idTicket: String, purchaseDate: String, sales: List[Sale]) extends BasicTicket {
    override def toString = "\n\tTicket_ID" + idTicket + "\tdate_achat" + purchaseDate + "\toffres" +sales
  }

  object SepClient {
    val ticketItemSep = "\002"

    def apply(res: Result): SepClient = {
      val id = Bytes.toString(res.getRow)
      res.raw.map(kv => Bytes.toString(kv.getValue)) match {
        case Array(tickets, age, idCardStatus, idGender, idSegmentRFM) =>
          SepClient(id, idGender.toInt, age.toInt, idCardStatus.toInt, idSegmentRFM.toInt, tickets.split(ticketItemSep).map(Ticket.deserialize).toList)
      }
    }
  }

  case class SepClient(id: String, idGender: Int, age: Int, idCardStatus :Int, idSegmentRFM :Int, tickets: List[Ticket])
    extends BasicClient with HBaseSerializable {

    val key = id.toString

    def dataBindings = List(
      // column-family / qualifier / value
      hbaseRowFormat("client", "Gender_id", idGender),
      hbaseRowFormat("client", "age", age),
      hbaseRowFormat("client", "Current_Card_Status_ID", idCardStatus),
      hbaseRowFormat("client", "RFM_Segment_ID", idSegmentRFM),
      hbaseRowFormat("achats", "achats", tickets.map(Ticket.serialize).mkString(SepClient.ticketItemSep)) // serialize to String
    )

    // This function is not necessary if we have already a map
    def mapPurchaseLines = {
      (id, idGender, age, idCardStatus, idSegmentRFM, tickets.map(l => (l.idTicket, l.purchaseDate, l.sales.map(pur => (pur.idProduct, pur.quantity, pur.price)))))
    }

  }

}

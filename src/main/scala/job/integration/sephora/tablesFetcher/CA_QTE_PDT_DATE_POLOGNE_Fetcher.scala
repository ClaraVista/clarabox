package job.integration.sephora.tablesFetcher

import fetcher.csvFetcher
import org.joda.time.DateTime
import job.integration.sephora.tools.StringToDate
import org.apache.spark.rdd.RDD

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 8/1/14
 * Time: 10:45 PM
 */
object CA_QTE_PDT_DATE_POLOGNE_Fetcher{

  case class Produit_Date_Quantity_CA(product_id: String, date: DateTime, quantity: Int, CA: Double)

  def generate_table(): RDD[Produit_Date_Quantity_CA]={
    //On a RDD[Array[Ticket_ID, Date_ID, Material_code, Material_ID, Quantity, Sales_VAT, Discount, nouvel_id_client]]
    val table_brut = csvFetcher.readData("/home/spark/workspace/IdeaProject/clarabox/data/Sephora/Lignesventes_anonymes0.csv"
                                        , has_header = true, delimiter = ",", string_delimiter = "\"")

    //Etape 1 : on a RDD[Array[Ticket_ID, Date_ID, Material_code, Material_ID, Quantity, Sales_VAT, Discount, nouvel_id_client]]
    //Etape 2 : on transforme en RDD[(material_id, date_id, quantity, sales_vat)]
    //Etape 3 : on groupBy (material_id, date_id)
    //Etape 4 : on transforme en RDD[Produit_Date_Quantity_CA]
    val result = table_brut
      .map(t => (t(3), t(1), t(4), t(5)))
      .groupBy(t => (t._1, t._2))
      .map(t => {
        val seq_quantity = t._2.map(p => if(p._4.toDouble == 0.0) 0 else if(p._4.toDouble < 0.0 && p._3.toInt > 0) p._3.toInt * -1 else p._3.toInt)
        val seq_ca = t._2.map(p => if(p._3.toInt == 0) 0 else if(p._3.toInt < 0 && p._4.toDouble > 0.0) p._4.toDouble * -1d else p._4.toDouble)

      Produit_Date_Quantity_CA(t._1._1, StringToDate.from_format_classic_to_datetime(t._1._2), seq_quantity.sum, seq_ca.sum)
    })

    result

  }


}

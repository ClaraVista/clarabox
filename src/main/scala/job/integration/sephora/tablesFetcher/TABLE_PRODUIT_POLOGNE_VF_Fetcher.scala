package job.integration.sephora.tablesFetcher

import fetcher.csvFetcher
import org.joda.time.DateTime
import job.integration.sephora.tools.StringToDate

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 8/1/14
 * Time: 11:36 PM
 */
object TABLE_PRODUIT_POLOGNE_VF_Fetcher{

  case class Product_complet(product_id: String, material_Description: String, material_Create_Date: DateTime, brand_Code: String
            , brand_Description: String, sub_Brand_Code: String, sub_Brand_Description: String, market_Code: String,
            market_Description: String, target_Code: String, target_Description: String, category_Code: String,
            category_Description: String, range_Code: String, range_Description: String, nature_Code: String,
            nature_Description: String, department_Code: String, department_Description: String, to_Excluded: String, axe: String)


  def generate_table()={
    val table_brute = csvFetcher.readData("/home/spark/workspace/IdeaProject/clarabox/data/Sephora/Table_pdt_france.csv"
                                          , has_header = true, delimiter = ";", string_delimiter = "\"")

    //Etape 1 : on a RDD[Array[string]]
    //Etape 2 : on transforme en RDD[Product_complet]
    //Etape 3 : on filter sur null
    //Etape 4 : on filter par les range code et to_excluded
    val result = table_brute.map(p =>{
      if(p(2) == ""){
        null
      }else{
        val date = StringToDate.from_format_classic_to_datetime(p(2))
        Product_complet(p(0), p(1), date, p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20))
      }
    })
    .filter(_ != null)
    .filter(p => p.to_Excluded == "0" && p.range_Code != "POUBELLE")

    result
  }
}

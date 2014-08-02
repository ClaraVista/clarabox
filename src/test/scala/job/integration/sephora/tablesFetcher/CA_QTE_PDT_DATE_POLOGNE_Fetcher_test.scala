package job.integration.sephora.tablesFetcher

import org.junit.Test

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 8/1/14
 * Time: 11:10 PM
 */
class CA_QTE_PDT_DATE_POLOGNE_Fetcher_test{

  @Test def get_table_test={
    val a = CA_QTE_PDT_DATE_POLOGNE_Fetcher.generate_table()

    val r = a.take(10)
    for(p <- r){
      println(p.product_id + "," + p.date.toDate.toString + "," + p.quantity + "," + p.CA + "\n")
    }
    assert(1 == 1)
  }
}

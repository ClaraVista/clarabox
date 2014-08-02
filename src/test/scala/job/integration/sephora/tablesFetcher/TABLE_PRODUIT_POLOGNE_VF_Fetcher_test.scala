package job.integration.sephora.tablesFetcher

import org.junit.Test

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 8/1/14
 * Time: 11:54 PM
 */
class TABLE_PRODUIT_POLOGNE_VF_Fetcher_test{

  @Test def generate_table_test()={

      val r = TABLE_PRODUIT_POLOGNE_VF_Fetcher.generate_table()

//    println(r.mkString("\n"))

      val l = r.map(p => p.market_Code).distinct().collect

      println(l.mkString(","))



  }
}

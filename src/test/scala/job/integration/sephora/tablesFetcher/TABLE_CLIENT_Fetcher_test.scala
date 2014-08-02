package job.integration.sephora.tablesFetcher

import org.junit.Test

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 8/2/14
 * Time: 8:24 PM
 */
class TABLE_CLIENT_Fetcher_test{


  @Test def generate_table_test()={
    val r = TABLE_CLIENT_Fetcher.generate_table()

//    val l = r.map(t => t.gender_ID).distinct.collect()
    val l = r.collect()
//    println(l.mkString("\n"))
  }
}

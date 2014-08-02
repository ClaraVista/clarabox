package fetcher

import org.junit.Test

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 7/31/14
 * Time: 2:21 PM
 */
class csvFetcherTest{

  @Test def readFileTest()={
      val rdd = csvFetcher.readData("/home/spark/workspace/IdeaProject/clarabox/src/test/scala/data/readFileTest.csv", has_header = true, delimiter = ",", string_delimiter = "\"")
    val res = List(List("a,b","1","2","3"), List("ab","1,2","3","4"), List("a", "b", "c", "d"))

    val res_rdd = rdd.collect().toList.map(_.toList)

//    println(res_rdd)
//    println(res)
    assert(res == res_rdd)
  }

  @Test def readFileTestSansHeader()={
    val rdd = csvFetcher.readData("/home/spark/workspace/IdeaProject/clarabox/src/test/scala/data/readFileTestSansHeader.csv", has_header = false, delimiter = ",", string_delimiter = "\"")


    val res = List(List("1", "2", "3", "4"), List( "a,b,c,d","2","3","4"), List("a", "b", "c", "d"))
    val res_rdd = rdd.collect().toList.map(_.toList)

//        println(res_rdd)
//        println(res)
    assert(res == res_rdd)
  }
}

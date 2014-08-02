package job.integration.sephora

import org.junit.Test
import fetcher.csvFetcher

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 7/17/14
 * Time: 5:52 PM
 */
class LauncherTest{

  @Test def categorisationTest()={
    val list_example = List(3,6,12,24)

    val a = launcher.categorization(2, list_example)
    assert(a == 0)

    val b = launcher.categorization(5, list_example)
    assert(b == 1)

    val c = launcher.categorization(20, list_example)
    assert(c == 3)

    val d = launcher.categorization(30, list_example)
    assert(d == 4)
  }

  @Test def famille_cat()={
    val rdd_famille = csvFetcher.readData("/home/spark/workspace/IdeaProject/clarabox/data/Sephora/Familles_olfactives.csv", has_header = true, delimiter = ",", string_delimiter = "\"")
//    .filter(_.size != 5)
      .map(t => (t(1), t(3))).filter(_._2.contains("FOUG�RE"))

//    println(rdd_famille.take(10).map(t => (t.toList, t.length)).toList.mkString("\n"))

    val l = rdd_famille.collect()
    val lf = l//.filter(_._2.contains("FOUG�RE"))
//
    println(lf.mkString(","))
  }

  @Test def remove_contenance_ml_material_description_test()={
    val test1 = "desc 10ml"
    val test2 = "desc prod 3000 25mL"
    val test3 = "desc prod 12.35ML"

    val res1 = launcher.remove_contenance_ml_material_description(test1)
    val res2 = launcher.remove_contenance_ml_material_description(test2)
    val res3 = launcher.remove_contenance_ml_material_description(test3)

    val tres1 = "desc"
    val tres2 = "desc prod 3000"
    val tres3 = "desc prod"

    assert(res1 == tres1)
    assert(res2 == tres2)
    assert(res3 == tres3)
  }

  @Test def dateTest()={
    val rdd = launcher.extractPurchaseLines()
    println(rdd.take(2))
  }
}

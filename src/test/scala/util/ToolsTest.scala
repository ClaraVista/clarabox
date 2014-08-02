package util

import org.junit.Test

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 7/17/14
 * Time: 4:33 PM
 */
class ToolsTest{

  @Test def concatenateTest()={
    val x = List(List(1, 2, 3), List(4, 5), List(6), List(), List(7, 8, 9, 10))
    val res = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val x2 = List(List(1, 2, 3, 4, 5))
    val res2 = List(1, 2, 3, 4, 5)
//    println(res)
//    println(Tools.concatener(x))
    assert(res == Tools.concatener(x))
    assert(res2 == Tools.concatener(x2))
  }
}

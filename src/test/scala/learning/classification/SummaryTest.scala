package learning.classification

import org.junit.Test
import learning.classification.output.{Summary, LabeledScore}

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 1/6/14
 * Time: 4:31 PM
 */
class SummaryTest {
  @Test def saveToStringTest() = {
    val scr = Summary(1, List(LabeledScore(1.0, 0.95), LabeledScore(0.0, 0.2), LabeledScore(1.0, 0.85)))
    assert(scr.toString == "1;1.0;0.95;0.0;0.2;1.0;0.85")
  }

  @Test def restoreTest() = {
    val scr = "1;1.0;0.95;0.0;0.2;1.0;0.85".split(";").toList
    assert(Summary.apply(scr).toString == "1;1.0;0.95;0.0;0.2;1.0;0.85")
  }
}

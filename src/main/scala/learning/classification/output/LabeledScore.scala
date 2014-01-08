package learning.classification.output

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 1/7/14
 * Time: 4:32 PM
 */
case class LabeledScore(label: Double, score: Double) {
  /**
   *
   * @return true if the individual is incorrectly classified, false if not
   */
  def isError: Boolean = {
    if (label == 1.0 && score < 0) true
    else if (label == 0.0 && score > 0) true
    else false
  }
}

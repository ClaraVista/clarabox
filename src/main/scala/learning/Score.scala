package learning

import org.apache.spark.util.Vector

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/16/13
 * Time: 5:56 PM
 */

case class Score(id: String, notes: Vector) {
  val sep = ";"
  override val toString = id + sep + notes.elements.mkString(sep)
}
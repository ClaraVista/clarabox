package serde

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/9/13
 * Time: 3:35 PM
 */
trait EntitySerde[T] {
  val fieldSep: String
  val listElemSep: String
  def serialize(obj: T): String
  def deserialize(str: String): T
}


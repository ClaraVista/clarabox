package component.browse

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/13/13
 * Time: 2:20 PM
 */
abstract class BasicVisit {
  val cookie: String
  val date: String
  val views: List[BasicView]
}

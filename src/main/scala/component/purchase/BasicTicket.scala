package component.purchase

/**
 * Created with IntelliJ IDEA.
 * User: hadoop
 * Date: 12/30/13
 * Time: 4:09 PM
 */
abstract class BasicTicket {
  val idTicket: String
  val purchaseDate: String
  val sales: List[BasicSale]

}

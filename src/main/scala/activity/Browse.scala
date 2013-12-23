package activity

import component.browse.BasicVisit

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/16/13
 * Time: 3:52 PM
 */
trait Browse extends Activity {
  val visits: List[BasicVisit]
}

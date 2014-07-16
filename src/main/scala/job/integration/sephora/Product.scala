package job.integration.sephora

import org.joda.time.DateTime
import component.BasicProduct

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 1/28/14
 * Time: 7:18 PM
 */
case class Product(idProduct :String, materialDescription :String, materialCreateTMSTP :String, brandCode :String, brandDescription :String, subBrandCode :String, subBrandDescription :String, marketCode :String, marketDescription :String, targetCode :String, targetDescription :String, categoryCode :String, categoryDescription :String, rangeCode :String, rangeDescription :String, natureCode :String, natureDescription :String, departmentCode :String, departmentDescription :String, toExcluded :String, axe :String) extends BasicProduct{


}

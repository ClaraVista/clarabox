package activity

import parameter.AnnouncerParams
import learning.Score

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/10/13
 * Time: 4:07 PM
 */

trait Activity {
  def retrieveNote(params: AnnouncerParams): Score
}




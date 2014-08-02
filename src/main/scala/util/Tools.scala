package util

import scala.annotation.tailrec

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 1/30/14
 * Time: 10:35 AM
 */
object Tools {

  /**
   * Concatene une liste de listes en une seule liste
   * @param param : une liste de listes
   * @return : une unique liste avec tous les éléments
   */
  def concatener (param : List[List[Any]]): List[Any] = {
    //On utilise un accumulateur pour avoir une fonction en tailrec, qui sont plus rapides
    @tailrec
    def inner(param: List[List[Any]], acc: List[Any]): List[Any] = param match {
      case Nil => acc
      case x :: xs => inner(xs, x.reverse ::: acc)
    }
    inner(param, Nil).reverse
  }

}


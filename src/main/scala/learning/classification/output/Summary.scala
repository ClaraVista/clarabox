package learning.classification.output

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 1/6/14
 * Time: 4:18 PM
 */
object Summary {

  def restoreScoreList(src: List[Double]): List[LabeledScore] = src match {
    case x1 :: x2 :: Nil => List(LabeledScore(x1, x2))
    case x1 :: x2 :: xs => LabeledScore(x1, x2) :: restoreScoreList(xs)
    case _ => throw new MatchError("score list not match")
  }

  def apply(src: List[String]): Summary = {
    Summary(src.head.toInt, restoreScoreList(src.tail.map(_.toDouble)))
  }
}

/**
 *
 * @param id the id of the individual
 * @param scoreList labeledScore for each label
 */
case class Summary(id: Int, scoreList: List[LabeledScore]) {

  val sep = ";"
  val tail = scoreList.toList.map {
    labScr => labScr.label + sep + labScr.score
  } mkString sep

  override def toString = id + sep + tail
}

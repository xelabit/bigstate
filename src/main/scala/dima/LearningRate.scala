package dima

import scala.math.pow

class LearningRate(val d: Int) {

  /**
    * Calculates possible choices for ε0.
    * @return a list of possible learning rates.
    */
  def makeInitalSeq(): List[Double] = {
    var lrs: List[Double] = List()
    for (i <- 0 until d) lrs :+= (1 / pow(2, i))
    lrs
  }

  def getLearningRatesForNextEpoch
}

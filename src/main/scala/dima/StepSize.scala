package dima

import scala.math.pow

/**
  * Manages a learning rate used for updating parameters from one epoch to another.
  * @param d worker parallelism.
  */
class StepSize(val d: Int) {
  var learningRate: Double = 0.0

  /**
    * Calculates possible choices for ε0.
    * @return a list of possible learning rates.
    */
  def makeInitialSeq(): Seq[Double] = {
    var lrs: Seq[Double] = Seq()
    for (i <- 0 until d) lrs :+= (1 / pow(2, i))
    lrs
  }

  /**
    * Given that a loss during the previous epoch has decreased and we haven't switched yet to a bold driver algorithm,
    * calculate a sequence of possible learning rates for the next epoch.
    * @return a sequence of learning rates.
    */
  def getLearningRatesForNextEpoch(): Seq[Double] = {
    val lrs = (0.5 to 2 by ((2 - 0.5) / (d - 1))).toSeq
    lrs.map(_ * learningRate)
  }

  /**
    * Increases current learning rate.
    */
  def incBoldDriver(): Unit = learningRate += learningRate * 0.05

  /**
    * Decreases current learning rate.
    */
  def decBoldDriver(): Unit = learningRate -= learningRate * 0.5
}

class MF {

}

object MF {
  val numFactors = 10
  val rangeMin = -0.1
  val rangeMax = 0.1

  /*
  TODO: according to DSGD this should be different for each epoch. ε0 is chosen among 1, 1/2, 1/4, ..., 1/2^(d-1), d is
  the number of worker nodes. Each of this value is tried in parallel (on a small subset of matrix V (~0.1%). The one
  which yields the smallest loss is chosen as ε0. If a loss decreased in the current epoch, in the next one we choose
  again in parallel among [1/2, 2] multiplied by the current learning rate. If the loss after the epoch has increased,
  we switch to "bold driver" algorithm: 1) increase the step size by 5% whenever the loss decreases over an epoch, 2)
  decrease the step size by 50% if the loss increases.
   */
  val learningRate = 0.01
}

package dima

import breeze.linalg._

class SGDUpdater(learningRate: Double) {

  /**
    * SGD for Matrix Factorization.
    * @param rating entry in matrix V.
    * @param user factor vector in matrix W.
    * @param item factor vector in matrix H.
    * @return updated factor vectors.
    */
  def delta(rating: Double, user: Array[Double], item: Array[Double]): (Array[Double], Array[Double]) = {
    val e = rating - user.zip(item).map { case (x, y) => x * y }.sum
    (item.map(i => learningRate * e * i), user.map(u => learningRate * e * u))
  }
}

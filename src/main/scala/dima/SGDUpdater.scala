package dima

import breeze.linalg._

class SGDUpdater(learningRate: Double) {

  /**
    * SGD for Matrix Factorization.
    * @param rating entry in matrix V.
    * @param user factor vector in matrix W.
    * @param item factor vector in matrix H.
    * @param n number of points in a stratum.
    * @return updated factor vectors.
    */
  def delta(rating: Double, user: Array[Double], item: Array[Double], n: Int): (Array[Double], Array[Double]) = {
    val w = new DenseVector(user)
    val h = new DenseVector(item)
    val wPrime = w - learningRate * n.toDouble * (-2.0) * (rating - w dot h) * h
    h        :-=     learningRate * n.toDouble * (-2.0) * (rating - w dot h) * w
    (wPrime.toArray, h.toArray)
  }
}

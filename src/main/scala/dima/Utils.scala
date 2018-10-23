package dima

import breeze.linalg.DenseVector
import breeze.numerics.pow
import dima.InputTypes.Rating
import dima.ps.Vector.Vector
import dima.ps.factors.SGDUpdater
import org.apache.flink.api.common.state.MapState

import scala.collection.mutable
import scala.util.Random

object Utils {
  type UserId = Int
  type ItemId = Int
  type D = Int
  type W = Long

//  // TODO: do not initialize factor matrices every time.
//  /**
//    * A function that takes a stream of ratings, performs gradient descent for factor matrices for each possible
//    * learning rate (defined in MF.learningRates) and outputs a stream either of updated user or item factor vectors.
//    * @param elements ratings in a current time window. We will sample from this data set.
//    */
//  def testLearningRatesOnSample(elements: Iterable[(Rating, D)], workerParallelism: Int, learningRates: Seq[Double],
//                                stepSize: StepSize): Unit = {
//    val ratings = elements.map(x => x._1)
//
//    /* In order to be able to test on a whole sample, we randomly choose a partition, from which we will sample. A
//       little bit artificial but should be sufficient for our purposes. */
//    val whichPartition = Random.nextInt(workerParallelism - 1)
//
//    // Select 0,01% of initial data at random in order to test various learning rates on it.
//    val sample = Random.shuffle(ratings.filter(x => x.userPartition == whichPartition).toList)
//      .drop((ratings.size * .999).toInt).iterator
//    val sgdUpdaters: Seq[SGDUpdater] = Seq.empty[SGDUpdater]
//    var k = 0
//    for (i <- learningRates) {
//      sgdUpdaters(k) = new SGDUpdater(i)
//      k += 1
//    }
//    val userVectors = new mutable.HashMap[UserId, Vector]
//    val itemVectors = new mutable.HashMap[ItemId, Vector]
//    val wMaps: Seq[mutable.HashMap[UserId, Vector]] = Seq.empty
//    val hMaps: Seq[mutable.HashMap[ItemId, Vector]] = Seq.empty
//
//    // Create hash maps (user/item factor matrices) for each learning rate.
//    for (i <- sgdUpdaters.indices) {
//      wMaps(i) = userVectors
//      hMaps(i) = itemVectors
//    }
//    for (rating <- sample) {
//      val out = learningRates.map(x => (x, rating))
//      var k = 0
//      for (i <- out) {
//        var w: Vector = wMaps(k)(rating.user)
//        var h: Vector = hMaps(k)(rating.item)
//        val (w0, h0) = sgdUpdaters(k).delta(rating.rating, w, h)
//        w = w0
//        h = h0
//        wMaps(k).update(rating.user, w)
//        hMaps(k).update(rating.item, h)
//        k += 1
//      }
//    }
//    stepSize.learningRate = getBestLearningRate(wMaps, hMaps, sample, learningRates)
//  }

//  /**
//    * Calculates losses for various learning rates.
//    * @param w a set of different user factor matrices, each corresponding to a different learning rate.
//    * @param h a set of different item factor matrices, each corresponding to a different learning rate.
//    * @param ratings true values from the initial matrix.
//    * @return a learning rate that resulted in a smallest loss.
//    */
//  def getBestLearningRate(w: Seq[mutable.HashMap[UserId, Vector]], h: Seq[mutable.HashMap[UserId, Vector]],
//                          ratings: Iterator[Rating], learningRates: Seq[Double]): Double = {
//    val losses: Seq[Double] = Seq.empty
//    var minLoss = 1000.0
//    var bestLearningRate: Double = null
//
//    // Size is equal to a number of different learning rates that are being tested.
//    for (i <- w.size) {
//      val wMatrix = w(i)
//      val hMatrix = h(i)
//      losses(i) = getLoss(wMatrix, hMatrix, ratings)
//      if (losses(i) < minLoss) {
//        minLoss = losses(i)
//        bestLearningRate = learningRates(i)
//      }
//    }
//    bestLearningRate
//  }

  /**
    * Calculates loss (difference between actual ratings and the ratings obtained from multiplying factor matrices).
    * @param w user factor matrix.
    * @param h item factor matrix.
    * @param ratings true values from matrix V.
    * @return single value of loss.
    */
  def getLoss(w: MapState[UserId, Vector], h: MapState[ItemId, Vector], ratings: Iterator[Rating]
             ): Double = {
    var loss = 0.0
    while (ratings.hasNext) {
      val rating = ratings.next()
      val wFactor = new DenseVector(w.get(rating.user))
      val hFactor = new DenseVector(h.get(rating.item))
      loss += pow(rating.rating - (wFactor dot hFactor), 2)
    }
    loss
  }

  def getHash(x: Int, partition: Int, flag: Boolean, partitionSize: Int, leftover: Int): Int = {
    var f = flag
    var p = partition
    var ps = partitionSize
    var l = leftover
    if (f) {
      ps += 1
      l -= 1
    }
    if (l > 0) f = true
    if (x <= ps) p
    else {
      ps -= 1
      ps += ps
      p += 1
      getHash(x, p, f, ps, l)
    }
  }

  /**
    * Get the number of a block (substratum), to which a record should be assigned.
    * @param id user/item id of a point.
    * @param maxId largest user/item id. We assume we know this value in advance. However, this is not usually the
    *              case in endless stream tasks.
    * @param n a size of parallelism.
    * @return an id of a substratum along user or item axis.
    */
  def partitionId(id: Int, maxId: Int, n: Int): Int = {
    val partitionSize = maxId / n
    val leftover = maxId - partitionSize * n
    val flag = leftover == 0
    if (id < maxId) getHash(id, 0, flag, partitionSize, leftover)
    else -1
  }
}

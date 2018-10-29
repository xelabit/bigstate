package dima.ps

import breeze.linalg.DenseVector
import breeze.numerics.pow
import dima.InputTypes.Rating
import dima.Utils._
import dima.ps.Vector._
import dima.ps.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}

import scala.collection.mutable
import scala.util.Random

class PSOnlineMatrixFactorizationWorker(numFactors: Int, rangeMin: Double, rangeMax: Double, learningRate: Double,
                                        userMemory: Int, negativeSampleRate: Int, maxItemId: Int, parallelism: Int
                                       ) extends WorkerLogic[(Rating, W), (ItemId, Int), Vector, (W, Double)] {
  val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  val seenItemsQueue = new mutable.HashMap[UserId, mutable.Queue[ItemId]]
  var currentWindow = 0L
  val factorUpdate = new SGDUpdater(learningRate)
  val ratingBuffer = new mutable.HashMap[ItemId, mutable.Queue[(Rating, W)]]()
  val seenItemsSet = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]
  val userVectors = new mutable.HashMap[UserId, Vector]
  val userLosses = new mutable.HashMap[UserId, Double]
  val itemIds = new mutable.ArrayBuffer[ItemId]

  def onPullRecv(paramId: (ItemId, Int), paramValue: Vector,
                 ps: ParameterServerClient[(ItemId, Int), Vector, (W, Double)]): Unit = {
    val rating = ratingBuffer synchronized ratingBuffer(paramId._1).dequeue()
    val user = userVectors.getOrElseUpdate(rating._1.user, factorInitDesc.open().nextFactor(rating._1.user))
    var item = paramValue
    val (userDelta, itemDelta) = factorUpdate.delta(rating._1.rating, user, item)
    userVectors(rating._1.user) = vectorSum(user, userDelta)
    item = vectorSum(item, itemDelta)
    if (currentWindow == 0L) currentWindow = rating._2
    else if (currentWindow < rating._2) {
      ps.output(rating._2, userLosses.values.sum)
      currentWindow = rating._2
      userLosses(rating._1.user) = getLoss(userVectors(rating._1.user), item, rating._1.rating)
    }
    else userLosses(rating._1.user) = getLoss(userVectors(rating._1.user), item, rating._1.rating)
    ps.push(paramId, itemDelta)
  }

  def onRecv(data: (Rating, W), ps: ParameterServerClient[(ItemId, Int), Vector, (W, Double)]): Unit = {
    val seenSet = seenItemsSet.getOrElseUpdate(data._1.user, new mutable.HashSet)
    val seenQueue = seenItemsQueue.getOrElseUpdate(data._1.user, new mutable.Queue)
    if (seenQueue.length >= userMemory) seenSet -= seenQueue.dequeue()
    seenSet += data._1.item
    seenQueue += data._1.item
    ratingBuffer synchronized {
      for(_ <- 1 to Math.min(itemIds.length - seenSet.size, negativeSampleRate)) {
        var randomItemId = itemIds(Random.nextInt(itemIds.size))
        while (seenSet contains randomItemId) randomItemId = itemIds(Random.nextInt(itemIds.size))
        ratingBuffer(randomItemId).enqueue((Rating(data._1.key, data._1.user, randomItemId, 0, data._1.timestamp,
          data._1.userPartition, data._1.itemPartition), data._2))
        ps.pull(randomItemId, partitionId(randomItemId, maxItemId, parallelism))
      }
      ratingBuffer.getOrElseUpdate(
        data._1.item,
        {
          itemIds += data._1.item
          mutable.Queue[(Rating, W)]()
        }).enqueue(data)
    }
    ps.pull(data._1.item, data._1.itemPartition)
  }

  def getLoss(w: Vector, h: Vector, r: Double): Double = {
    val wFactor = new DenseVector(w)
    val hFactor = new DenseVector(h)
    pow(r - (wFactor dot hFactor), 2)
  }
}

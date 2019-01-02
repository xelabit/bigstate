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
                                       ) extends WorkerLogic[(Rating, W), (ItemId, Int), Vector, (String, W, Double)] {
  val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  val seenItemsQueue = new mutable.HashMap[UserId, mutable.Queue[ItemId]]
  var currentWindow = 0L
  var currentTestWindow = 0L
  val factorUpdate = new SGDUpdater(learningRate)
  val ratingBuffer = new mutable.HashMap[ItemId, mutable.Queue[(Rating, W)]]()
  val seenItemsSet = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]
  val userVectors = new mutable.HashMap[UserId, Vector]
//  val losses = new mutable.HashMap[(UserId, ItemId), Double]
//  val testLosses = new mutable.HashMap[(UserId, ItemId), Double]
  var trainLoss = 0.0
  var testLoss = 0.0
  val itemIds = new mutable.ArrayBuffer[ItemId]

  def onPullRecv(paramId: (ItemId, Int), paramValue: Vector,
                 ps: ParameterServerClient[(ItemId, Int), Vector, (String, W, Double)]): Unit = {
    val rating = ratingBuffer synchronized ratingBuffer(paramId._1).dequeue()
    val user = userVectors.getOrElseUpdate(rating._1.user, factorInitDesc.open().nextFactor(rating._1.user))
    var item = paramValue
    rating._1.label match {
      case "train" => {
        val (userDelta, itemDelta) = factorUpdate.delta(rating._1.rating, user, item)
        userVectors(rating._1.user) = vectorSum(user, userDelta)
        item = vectorSum(item, itemDelta)
        if (currentWindow == 0L) currentWindow = rating._2
        else if (currentWindow < rating._2) {
//          ps.output("train", currentWindow, losses.values.sum)
          ps.output("train", currentWindow, trainLoss)
          currentWindow = rating._2
//          losses((rating._1.user, rating._1.item)) = getLoss(userVectors(rating._1.user), item, rating._1.rating)
          trainLoss += getLoss(userVectors(rating._1.user), item, rating._1.rating)
        }
//        else losses((rating._1.user, rating._1.item)) = getLoss(userVectors(rating._1.user), item, rating._1.rating)
        else trainLoss += getLoss(userVectors(rating._1.user), item, rating._1.rating)
        ps.push(paramId, itemDelta)
      }
      case "test" => {
        if (currentTestWindow == 0L) currentTestWindow = rating._2
        else if (currentTestWindow < rating._2) {
//          ps.output("test", currentTestWindow, testLosses.values.sum)
          ps.output("test", currentTestWindow, testLoss)
          currentTestWindow = rating._2
//          testLosses((rating._1.user, rating._1.item)) = getLoss(userVectors(rating._1.user), item, rating._1.rating)
          testLoss += getLoss(userVectors(rating._1.user), item, rating._1.rating)
        }
//        else testLosses((rating._1.user, rating._1.item)) = getLoss(userVectors(rating._1.user), item, rating._1.rating)
        else testLoss += getLoss(userVectors(rating._1.user), item, rating._1.rating)
      }
    }
  }

  def onRecv(data: (Rating, W), ps: ParameterServerClient[(ItemId, Int), Vector, (String, W, Double)]): Unit = {
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
          data._1.userPartition, data._1.itemPartition, data._1.label), data._2))
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

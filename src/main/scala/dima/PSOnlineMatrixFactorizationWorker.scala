package dima

import dima.Utils.{ItemId, UserId, W}
import dima.Vector._

import scala.collection.mutable
import scala.util.Random

class PSOnlineMatrixFactorizationWorker(numFactors: Int, rangeMin: Double, rangeMax: Double, learningRate: Double,
                                        userMemory: Int, negativeSampleRate: Int
                                       ) extends WorkerLogic[(Rating, W), Vector, (W, UserId, Vector)] {
  val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  val factorUpdate = new SGDUpdater(learningRate)
  val userVectors = new mutable.HashMap[UserId, Vector]
  val ratingBuffer = new mutable.HashMap[ItemId, mutable.Queue[(Rating, W)]]()
  val itemIds = new mutable.ArrayBuffer[ItemId]
  val seenItemsSet = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]
  val seenItemsQueue = new mutable.HashMap[UserId, mutable.Queue[ItemId]]

  def onPullRecv(paramId: ItemId, paramValue: Vector, ps: ParameterServerClient[Vector, (W, UserId, Vector)]): Unit = {
    val rating = ratingBuffer synchronized ratingBuffer(paramId).dequeue()
    val user = userVectors.getOrElseUpdate(rating._1.user, factorInitDesc.open().nextFactor(rating._1.user))
    val item = paramValue
    val (userDelta, itemDelta) = factorUpdate.delta(rating._1.rating, user, item)
    userVectors(rating._1.user) = vectorSum(user, userDelta)
    ps.output(rating._2, rating._1.user, userVectors(rating._1.user))
    ps.push(paramId, itemDelta)
  }

  def onRecv(data: (Rating, W), ps: ParameterServerClient[Vector, (W, UserId, Vector)]): Unit = {
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
        ps.pull(randomItemId)
      }
      ratingBuffer.getOrElseUpdate(
        data._1.item,
        {
          itemIds += data._1.item
          mutable.Queue[(Rating, W)]()
        }).enqueue(data)
    }
    ps.pull(data._1.item)
  }
}

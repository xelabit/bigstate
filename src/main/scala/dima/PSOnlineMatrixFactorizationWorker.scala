package dima

import dima.Utils.{ItemId, UserId}
import dima.Vector._

import scala.collection.mutable
import scala.util.Random

class PSOnlineMatrixFactorizationWorker(numFactors: Int, rangeMin: Double, rangeMax: Double, learningRate: Double,
                                        stratumSize: Int, userMemory: Int, negativeSampleRate: Int
                                       ) extends WorkerLogic[Rating, Vector, (UserId, Vector)] {
  val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  val factorUpdate = new SGDUpdater(learningRate)
  val userVectors = new mutable.HashMap[UserId, Vector]
  val ratingBuffer = new mutable.HashMap[ItemId, mutable.Queue[Rating]]()
  val itemIds = new mutable.ArrayBuffer[ItemId]
  val seenItemsSet = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]
  val seenItemsQueue = new mutable.HashMap[UserId, mutable.Queue[ItemId]]

  def onPullRecv(paramId: ItemId, paramValue: Vector, ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
    val rating = ratingBuffer synchronized ratingBuffer(paramId).dequeue()
    val user = userVectors.getOrElseUpdate(rating.user, factorInitDesc.open().nextFactor(rating.user))
    val item = paramValue
    val (userDelta, itemDelta) = factorUpdate.delta(rating.rating, user, item, stratumSize)
    userVectors(rating.user) = vectorSum(user, userDelta)
    ps.output(rating.user, userVectors(rating.user))
    ps.push(paramId, itemDelta)
  }

  def onRecv(data: Rating, ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
    val seenSet = seenItemsSet.getOrElseUpdate(data.user, new mutable.HashSet)
    val seenQueue = seenItemsQueue.getOrElseUpdate(data.user, new mutable.Queue)
    if (seenQueue.length >= userMemory) seenSet -= seenQueue.dequeue()
    seenSet += data.item
    seenQueue += data.item
    ratingBuffer synchronized {
      for(_ <- 1 to Math.min(itemIds.length - seenSet.size, negativeSampleRate)) {
        var randomItemId = itemIds(Random.nextInt(itemIds.size))
        while (seenSet contains randomItemId) randomItemId = itemIds(Random.nextInt(itemIds.size))
        ratingBuffer(randomItemId).enqueue(Rating(data.key, data.user, randomItemId, 0, data.timestamp,
          data.userPartition, data.itemPartition))
        ps.pull(randomItemId)
      }
      ratingBuffer.getOrElseUpdate(
        data.item,
        {
          itemIds += data.item
          mutable.Queue[Rating]()
        }).enqueue(data)
    }
    ps.pull(data.item)
  }
}

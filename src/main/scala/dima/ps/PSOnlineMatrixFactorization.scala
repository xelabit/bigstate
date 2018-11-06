package dima.ps

import dima.InputTypes.Rating
import dima.Utils.{ItemId, UserId, W}
import dima.ps.Vector._
import dima.ps.factors.RangedRandomFactorInitializerDescriptor
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

class PSOnlineMatrixFactorization {

}

object PSOnlineMatrixFactorization {

  def psOnlineMF(src: DataStream[(Rating, W)], numFactors: Int = 10, rangeMin: Double = -0.01,
                 rangeMax: Double = 0.01, learningRate: Double, userMemory: Int,
                 negativeSampleRate: Int, pullLimit: Int = 1600, workerParallelism: Int, psParallelism: Int,
                 iterationWaitTime: Long = 10000, maxItemId: Int
                ): DataStream[Either[(String, W, Double), ((ItemId, Int), Vector)]] = {
    val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
    val workerLogicBase = new PSOnlineMatrixFactorizationWorker(numFactors, rangeMin, rangeMax, learningRate,
      userMemory, negativeSampleRate, maxItemId, psParallelism)
    val workerLogic: WorkerLogic[(Rating, W), (ItemId, Int), Vector, (String, W, Double)] =
      WorkerLogic.addPullLimiter(workerLogicBase, pullLimit)
    val serverLogic = new SimplePSLogic[(ItemId, Int), Array[Double]](x => factorInitDesc.open().nextFactor(x._1),
      (vec, deltaVec) => vectorSum(vec, deltaVec))
    val partitionedInput = src
      .partitionCustom(new Partitioner[Int] {
        override def partition(key: UserId, numPartitions: ItemId): ItemId = key
      }, x => x._1.userPartition)
    val modelUpdates = FlinkParameterServer.transform(partitionedInput, workerLogic, serverLogic, workerParallelism,
      psParallelism, iterationWaitTime)
    modelUpdates
  }
}

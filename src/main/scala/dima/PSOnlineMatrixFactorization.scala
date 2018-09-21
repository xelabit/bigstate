package dima

import dima.Utils.{ItemId, UserId}
import dima.Vector._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class PSOnlineMatrixFactorization {

}

object PSOnlineMatrixFactorization {

  def psOnlineMF(src: DataStream[Rating], numFactors: Int = 10, rangeMin: Double = -0.01,
                 rangeMax: Double = 0.01, learningRate: Double, stratumSize: Int, userMemory: Int,
                 negativeSampleRate: Int, pullLimit: Int = 1600, workerParallelism: Int, psParallelism: Int,
                 iterationWaitTime: Long = 10000): DataStream[Either[(UserId, Vector), (ItemId, Vector)]] = {
    val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
    val workerLogicBase = new PSOnlineMatrixFactorizationWorker(numFactors, rangeMin, rangeMax, learningRate,
      stratumSize, userMemory, negativeSampleRate)
    val workerLogic: WorkerLogic[Rating, Vector, (UserId, Vector)] =
      WorkerLogic.addPullLimiter(workerLogicBase, pullLimit)
    val serverLogic = new SimplePSLogic[Array[Double]](
      x => factorInitDesc.open().nextFactor(x), { (vec, deltaVec) => vectorSum(vec, deltaVec) }
    )
    val partitionedInput = src.partitionCustom(new Partitioner[Int] {

      override def partition(key: UserId, numPartitions: Int): ItemId = { key }
    }, x => x.userPartition)
    val modelUpdates = FlinkParameterServer.transform(partitionedInput, workerLogic, serverLogic, workerParallelism,
      psParallelism, iterationWaitTime)
    modelUpdates
  }
}

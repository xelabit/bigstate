package dima

import dima.Utils.{ItemId, UserId, W}
import dima.Vector._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class PSOnlineMatrixFactorization {

}

object PSOnlineMatrixFactorization {

  def psOnlineMF(src: DataStream[(Rating, W)], numFactors: Int = 10, rangeMin: Double = -0.01,
                 rangeMax: Double = 0.01, learningRate: Double, userMemory: Int,
                 negativeSampleRate: Int, pullLimit: Int = 1600, workerParallelism: Int, psParallelism: Int,
                 iterationWaitTime: Long = 10000): DataStream[Either[(W, UserId, Vector), (ItemId, Vector)]] = {
    val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
    val workerLogicBase = new PSOnlineMatrixFactorizationWorker(numFactors, rangeMin, rangeMax, learningRate,
      userMemory, negativeSampleRate)
    val workerLogic: WorkerLogic[(Rating, W), Vector, (W, UserId, Vector)] =
      WorkerLogic.addPullLimiter(workerLogicBase, pullLimit)
    val serverLogic = new SimplePSLogic[Array[Double]](x => factorInitDesc.open().nextFactor(x),
      (vec, deltaVec) => vectorSum(vec, deltaVec))
    val partitionedInput = src
//      .map(x => x._1)
      .partitionCustom((key: UserId, numPartitions: Int) => key, x => x._1.userPartition)
    val modelUpdates = FlinkParameterServer.transform(partitionedInput, workerLogic, serverLogic, workerParallelism,
      psParallelism, iterationWaitTime)
    modelUpdates
  }
}

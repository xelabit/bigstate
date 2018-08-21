import dima.Utils.{ItemId, UserId}
import dima.{PSOnlineMatrixFactorization, Rating, StepSize}
import dima.Vector._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable

class MF {

}

object MF {
  val numFactors = 10
  val rangeMin = -0.1
  val rangeMax = 0.1
  val workerParallelism = 2
  val psParallelism = 2

  /*
  TODO: according to DSGD this should be different for each epoch. ε0 is chosen among 1, 1/2, 1/4, ..., 1/2^(d-1), d is
  the number of worker nodes. Each of this value is tried in parallel (on a small subset of matrix V (~0.1%). The one
  which yields the smallest loss is chosen as ε0. If a loss decreased in the current epoch, in the next one we choose
  again in parallel among [1/2, 2] multiplied by the current learning rate. If the loss after the epoch has increased,
  we switch to "bold driver" algorithm: 1) increase the step size by 5% whenever the loss decreases over an epoch, 2)
  decrease the step size by 50% if the loss increases.
   */
  val stepSize = new StepSize(workerParallelism)
  val learningRates = stepSize.makeInitialSeq()
  val learningRate = 0.01
  val pullLimit = 1500
  val iterationWaitTime = 10000

  def main(args: Array[String]): Unit = {
    val input_file_name = args(0)
    val userVector_output_name = args(1)
    val itemVector_output_name = args(2)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val data = env.readTextFile(input_file_name)
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
    val lastFM = data.flatMap(new RichFlatMapFunction[String, Rating] {

      override def flatMap(value: String, out: Collector[Rating]): Unit = {
        val fieldsArray = value.split(",")
        val r = Rating(fieldsArray(1).toInt, fieldsArray(2).toInt, fieldsArray(3).toInt,
          formatter.parseDateTime(fieldsArray(0)))
        out.collect(r)
      }
    })
      .assignAscendingTimestamps(_.timestamp.getMillis)
      .keyBy(_.user)
      .window(TumblingEventTimeWindows.of(Time.days(1)))

    // TODO: before calling psOnlineMF, a keyed stream should be created.
    PSOnlineMatrixFactorization.psOnlineMF(lastFM, numFactors, rangeMin, rangeMax, learningRate, pullLimit,
      workerParallelism, psParallelism, iterationWaitTime)
      .addSink(new RichSinkFunction[Either[(UserId, Vector), (ItemId, Vector)]] {
        val userVectors = new mutable.HashMap[UserId, Vector]
        val itemVectors = new mutable.HashMap[ItemId, Vector]

        override def invoke(value: Either[(UserId, Vector), (ItemId, Vector)]): Unit = {
          value match {
            case Left((userId, vec)) => userVectors.update(userId, vec)
            case Right((itemId, vec)) => itemVectors.update(itemId, vec)
          }
        }

        override def close(): Unit = {
          val userVectorFile = new java.io.PrintWriter(new java.io.File(userVector_output_name))
          for ((k, v) <- userVectors) for (value <- v) userVectorFile.write(k + ";" + value + '\n')
          userVectorFile.close()
          val itemVectorFile = new java.io.PrintWriter(new java.io.File(itemVector_output_name))
          for ((k, v) <- itemVectors) for (value <- v) itemVectorFile.write(k + ";" + value + '\n')
          itemVectorFile.close()
        }
      }).setParallelism(1)
    env.execute()
  }
}

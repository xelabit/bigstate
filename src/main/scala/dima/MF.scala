import dima.{Rating, StepSize}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.joda.time.format.DateTimeFormat

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
  val pullLimit = 1500
  val iterationWaitTime = 10000

  def main(args: Array[String]): Unit = {
    val input_file_name = args(0)
    val userVector_output_name = args(1)
    val itemVector_output_name = args(2)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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

    // TODO: before calling psOnlineMF, a keyed stream should be created.
  }
}

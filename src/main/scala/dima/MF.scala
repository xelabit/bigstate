import dima.Utils.{ItemId, UserId}
import dima.{PSOnlineMatrixFactorization, Rating, StepSize}
import dima.Vector._
import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable
import scala.util.Random
import scala.util.control.Breaks._

class MF {

}

object MF {
  val numFactors = 10
  val rangeMin = -0.1
  val rangeMax = 0.1

  // TODO: replace 10000 with real values.
  val maxUId = 10000
  val maxIId = 10000
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
  val learningRates: Seq[Double] = stepSize.makeInitialSeq()
  val learningRate = 0.01
  val pullLimit = 1500
  val iterationWaitTime = 10000
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {
    val input_file_name = args(0)
    val userVector_output_name = args(1)
    val itemVector_output_name = args(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val data = env.readTextFile(input_file_name)
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
    val lastFM = data.flatMap(new RichFlatMapFunction[String, Rating] {

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
        if (id < maxId) getHash(id,0, flag, partitionSize, leftover)
        else -1
      }

      override def flatMap(value: String, out: Collector[Rating]): Unit = {
        val fieldsArray = value.split(",")
        val uid = fieldsArray(2).toInt
        val iid = fieldsArray(3).toInt
        val userPartition = partitionId(uid, maxUId, workerParallelism)
        val itemPartition = partitionId(iid, maxIId, workerParallelism)
        val r = Rating(fieldsArray(0).toInt, uid, iid, fieldsArray(4).toInt,
          formatter.parseDateTime(fieldsArray(1)), userPartition, itemPartition)
        out.collect(r)
      }
    })
      .assignAscendingTimestamps(_.timestamp.getMillis)
      .keyBy(_.key)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .process(new DSGD())

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

/**
  * A main function, inside which the PS data flows are taking place. Once having obtained enough ratings in a window,
  * we start the usual process of calling of PS APIs. We need such extra window abstraction in order to be able to
  * process our ratings several times (which corresponds to the number of epochs according to DSGD algorithm) with
  * different learning rates.
  */
class DSGD extends ProcessWindowFunction[Rating, Either[(UserId, Vector), (ItemId, Vector)], Int, TimeWindow] {
  private var userState: MapState[UserId, Vector] = _
  private var itemState: MapState[ItemId, Vector] = _
  private var userStateDescriptor: MapStateDescriptor[UserId, Vector] = _
  private var itemStateDescriptor: MapStateDescriptor[ItemId, Vector] = _

  /**
    * Calculates the blocks that will form a stratum.
    * @param strategy either 0 (if a possible stratum is defined as a set of blocks (0,0), (1,1), ..., (n,n)) or 1
    *                 (possible stratum is (n,0), (n-1,2), ..., (1, n))
    * @param substrategy one of the possible ways to define a stratum. We will have as many different substrategies,
    *                    as the number of parallelism defined for the job. The number of substrategy defines the block
    *                    along the user axis.
    * @return two sequences of block ids along user/item axis. These sequences are essentially pairs of coordinates,
    *         that define blocks of a stratum.
    */
  def getStratum(strategy: Int, substrategy: Int): (Seq[Int], Seq[Int]) = {
    if (strategy != 0 || strategy != 1) throw IllegalArgumentException
    var s = substrategy
    val iBlocks = 0 to MF.workerParallelism
    var uBlocks: Seq[Int] = Seq.empty[Int]
    for (i <- 0 until MF.workerParallelism) {
      uBlocks :+ s
      strategy match {
        case 0 =>
          s += 1
          if (s > MF.workerParallelism) s = 0
        case 1 =>
          s -= 1
          if (s < 0) s = MF.workerParallelism
      }
    }
    (uBlocks, iBlocks)
  }

  override def process(key: Int, context: Context, elements: Iterable[Rating],
                       out: Collector[Either[(UserId, Vector), (ItemId, Vector)]]): Unit = {
    userState = context.globalState.getMapState(userStateDescriptor)
    itemState = context.globalState.getMapState(itemStateDescriptor)

    // Select 0,01% of initial data at random in order to test various learning rates on it.
    val sample = Random.shuffle(elements.toList).drop((elements.size * .999).toInt).iterator

    // "Double" is a learning rate.
    val sampleStream: DataStream[(Double, Rating)] = MF.env.fromCollection(sample)
      .flatMap(new LearningRateFlatMapFunction)


    var hasConverged = false
    while (!hasConverged) {
      val ratings = Random.shuffle(elements.toList).iterator
      val strategy = Random.nextInt(2)
      val substrategies = 0 to MF.workerParallelism

      /* The number of subepochs is equal to the number of partitions of original matrix or, in other words, to the
         number of worker nodes. */
      for (i <- 0 until MF.workerParallelism) {
        val substrategy = Random.shuffle(substrategies.toList).head
        substrategies.drop(1)
        val blocks = getStratum(strategy, substrategy)
        val lastFM: DataStream[Rating] = MF.env.fromCollection(ratings)
          .filter(new StratumFilterFunction(blocks))


      }
    }
  }

  override def open(parameters: Configuration): Unit = {
    userStateDescriptor = new MapStateDescriptor[UserId, Vector]("user-state", createTypeInformation[UserId],
      createTypeInformation[Vector])
    itemStateDescriptor = new MapStateDescriptor[ItemId, Vector]("item-state", createTypeInformation[ItemId],
      createTypeInformation[Vector])
  }
}

class StratumFilterFunction(blocks: (Seq[Int], Seq[Int])) extends RichFilterFunction[Rating] {

  /**
    * We will work only with points in a selected stratum.
    * @param t element of a stream.
    * @return true if an element belongs to one of the blocks in a stratum.
    */
  override def filter(t: Rating): Boolean = {
    var f = false
    for (i <- 0 until MF.workerParallelism) {
      if (t.userPartition == blocks._1(i) && t.itemPartition == blocks._2(i)) {
        f = true
        break
      }
    }
    f
  }
}

class LearningRateFlatMapFunction extends RichFlatMapFunction[Rating, (Double, Rating)] {
  override def flatMap(in: Rating, collector: Collector[(Double, Rating)]): Unit = {
    val out = MF.learningRates.map(x => (x, in))
    for (i <- out.indices) collector.collect(out(i))
  }
}
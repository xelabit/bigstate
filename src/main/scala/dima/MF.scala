import MF._
import breeze.linalg.DenseVector
import breeze.numerics.{abs, pow}
import dima.Utils.{D, ItemId, UserId}
import dima.{PSOnlineMatrixFactorization, Rating, SGDUpdater, StepSize, Utils}
import dima.Vector._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, RichWindowFunction}
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
  val userMemory = 128
  val negativeSampleRate = 9

  // TODO: replace 10000 with real values.
  val maxUId = 10000
  val maxIId = 10000
  val workerParallelism = 2
  val psParallelism = 2

  /* According to DSGD this should be different for each epoch. ε0 is chosen among 1, 1/2, 1/4, ..., 1/2^(d-1), d is
     the number of worker nodes. Each of this value is tried in parallel (on a small subset of matrix V (~0.1%). The one
     which yields the smallest loss is chosen as ε0. If a loss decreased in the current epoch, in the next one we choose
     again in parallel among [1/2, 2] multiplied by the current learning rate. If the loss after the epoch has
     increased, we switch to "bold driver" algorithm: 1) increase the step size by 5% whenever the loss decreases over
     an epoch, 2) decrease the step size by 50% if the loss increases. */
  val stepSize = new StepSize(workerParallelism)
  var learningRates: Seq[Double] = stepSize.makeInitialSeq()
  val learningRate = 0.01
  var n = 0
  val pullLimit = 1500
  val iterationWaitTime = 10000

  def epoch(ratings: DataStream[(Rating, D)]): (DataStream[(Rating, D)], DataStream[(Rating, D)]) = {
    var hasConverged = false
    val stratumStream = ratings
      .keyBy(_._1.key)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .apply(new SortSubstratums)
    val factorStream = PSOnlineMatrixFactorization.psOnlineMF(stratumStream, numFactors, rangeMin, rangeMax,
      stepSize.learningRate, n, userMemory, negativeSampleRate, pullLimit, workerParallelism, psParallelism,
      iterationWaitTime)
    val lossStream = stratumStream
      .connect(factorStream)
      .process(new CoProcessFunction[Rating, Either[(UserId, Vector), (ItemId, Vector)], (Int, Double)] {
        val userVectors = new mutable.HashMap[UserId, Vector]
        val itemVectors = new mutable.HashMap[ItemId, Vector]
        var ratings: Seq[Rating] = Seq.empty
        var epoch = 0
        var epochLoss = 0.0

        override def processElement1(in1: Rating,
                                     context: CoProcessFunction[Rating, Either[(UserId, Vector), (ItemId, Vector)],
                                       (Int, Double)]#Context, collector: Collector[(Int, Double)]): Unit = {
          ratings = ratings.+:(in1)
        }

        override def processElement2(in2: Either[(UserId, Vector), (ItemId, Vector)],
                                     context: CoProcessFunction[Rating, Either[(UserId, Vector), (ItemId, Vector)],
                                       (Int, Double)]#Context, collector: Collector[(Int, Double)]): Unit = {
          in2 match {
            case Left((userId, vec)) => userVectors.update(userId, vec)
            case Right((itemId, vec)) => itemVectors.update(itemId, vec)
          }
        }

        override def close(): Unit = {
          epochLoss = Utils.getLoss(userVectors, itemVectors, ratings.iterator)
        }
      })
      .keyBy(_._1)
    val epochLosses = lossStream
      .sum(2)
      .setParallelism(1)
      .flatMap(new RichFlatMapFunction[(Int, Double), Double] {
      var lastEpochLoss: Double = null
      var isFirstEpoch = true
      var isBoldDriver = false
      val threshold = 0.01

      override def flatMap(in: (ItemId, Double), collector: Collector[Double]): Unit = {
        if (isFirstEpoch) isFirstEpoch = false
        else if (!isBoldDriver) {
          if (in._2 < lastEpochLoss) {
            MF.learningRates = stepSize.getLearningRatesForNextEpoch()
//            testLearningRatesOnSample(elements)
          } else {
              isBoldDriver = true
              if (in._2 < lastEpochLoss) stepSize.incBoldDriver()
              else stepSize.decBoldDriver()
            }
        }
        if (abs(in._2 - lastEpochLoss) < threshold) hasConverged = true
        else lastEpochLoss = in._2
      }
    })
    if (hasConverged) (, ratings)
    else (ratings, )
  }

  def main(args: Array[String]): Unit = {
    val input_file_name = args(0)
    val userVector_output_name = args(1)
    val itemVector_output_name = args(2)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val data = env.readTextFile(input_file_name)
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
    val lastFM = data.flatMap(new RichFlatMapFunction[String, (Rating, D)] {

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
        if (id < maxId) getHash(id, 0, flag, partitionSize, leftover)
        else -1
      }

      override def flatMap(value: String, out: Collector[(Rating, D)]): Unit = {
        var distance = 0
        val fieldsArray = value.split(",")
        val uid = fieldsArray(2).toInt
        val iid = fieldsArray(3).toInt
        val userPartition = partitionId(uid, maxUId, workerParallelism)
        val itemPartition = partitionId(iid, maxIId, workerParallelism)
        val r = Rating(fieldsArray(0).toInt, uid, iid, fieldsArray(4).toInt,
          formatter.parseDateTime(fieldsArray(1)), userPartition, itemPartition)
        if (r.userPartition != r.itemPartition) {
          distance = r.itemPartition - r.userPartition
          if (distance < 0) distance += MF.workerParallelism
        }
        out.collect((r, distance))
      }
    })
      .assignAscendingTimestamps(_._1.timestamp.getMillis)
      .iterate((x: DataStream[(Rating, D)]) => epoch(x), 1000)
    env.execute()
  }
}

/**
  * A main function, inside which the PS data flows are taking place. Once having obtained enough ratings in a window,
  * we start the usual process of calling of PS APIs. We need such extra window abstraction in order to be able to
  * process our ratings several times (which corresponds to the number of epochs according to DSGD algorithm) with
  * different learning rates.
  */
class DSGD extends ProcessWindowFunction[Rating, Rating, Int, TimeWindow] {
  private var userState: MapState[UserId, Vector] = _
  private var itemState: MapState[ItemId, Vector] = _
  private var userStateDescriptor: MapStateDescriptor[UserId, Vector] = _
  private var itemStateDescriptor: MapStateDescriptor[ItemId, Vector] = _

  // If the difference between the losses of two subsequent epochs is smaller than the threshold, we stop the training.
  val threshold = 0.01

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
    val uBlocks: Seq[Int] = Seq.empty[Int]
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

  /**
    * Calculates losses for various learning rates.
    * @param w a set of different user factor matrices, each corresponding to a different learning rate.
    * @param h a set of different item factor matrices, each corresponding to a different learning rate.
    * @param ratings true values from the initial matrix.
    * @return a learning rate that resulted in a smallest loss.
    */
  def getBestLearningRate(w: Seq[mutable.HashMap[UserId, Vector]], h: Seq[mutable.HashMap[UserId, Vector]],
                ratings: Iterator[Rating]): Double = {
    val losses: Seq[Double] = Seq.empty
    var minLoss = 1000.0
    var bestLearningRate: Double = null

    // Size is equal to a number of different learning rates that are being tested.
    for (i <- w.size) {
      val wMatrix = w(i)
      val hMatrix = h(i)
      losses(i) = getLoss(wMatrix, hMatrix, ratings)
      if (losses(i) < minLoss) {
        minLoss = losses(i)
        bestLearningRate = MF.learningRates(i)
      }
    }
    bestLearningRate
  }

  /**
    * Calculates loss (difference between actual ratings and the ratings obtained from multiplying factor matrices).
    * @param w user factor matrix.
    * @param h item factor matrix.
    * @param ratings true values from matrix V.
    * @return single value of loss.
    */
  def getLoss(w: mutable.HashMap[UserId, Vector], h: mutable.HashMap[ItemId, Vector], ratings: Iterator[Rating]
             ): Double = {
    var loss = 0
    while (ratings.hasNext) {
      val rating = ratings.next()
      val wFactor = new DenseVector(w(rating.user))
      val hFactor = new DenseVector(h(rating.item))
      loss += pow(rating.rating - (wFactor dot hFactor), 2)
    }
    loss
  }

  // TODO: do not initialize factor matrices every time.
  /**
    * A function that takes a stream of ratings, performs gradient descent for factor matrices for each possible
    * learning rate (defined in MF.learningRates) and outputs a stream either of updated user or item factor vectors.
    * @param elements ratings in a current time window. We will sample from this data set.
    */
  def testLearningRatesOnSample(elements: Iterable[Rating]): Unit = {

    /* In order to be able to test on a whole sample, we randomly choose a partition, from which we will sample. A
       little bit artificial but should be sufficient for our purposes. */
    val whichPartition = Random.nextInt(MF.workerParallelism - 1)

    // Select 0,01% of initial data at random in order to test various learning rates on it.
    val sample = Random.shuffle(elements.filter(x => x.userPartition == whichPartition).toList)
      .drop((elements.size * .999).toInt).iterator
    val sgdUpdaters: Seq[SGDUpdater] = Seq.empty[SGDUpdater]
    var k = 0
    for (i <- MF.learningRates) {
      sgdUpdaters(k) = new SGDUpdater(i)
      k += 1
    }
    val userVectors = new mutable.HashMap[UserId, Vector]
    val itemVectors = new mutable.HashMap[ItemId, Vector]
    val wMaps: Seq[mutable.HashMap[UserId, Vector]] = Seq.empty
    val hMaps: Seq[mutable.HashMap[ItemId, Vector]] = Seq.empty

    // Create hash maps (user/item factor matrices) for each learning rate.
    for (i <- sgdUpdaters.indices) {
      wMaps(i) = userVectors
      hMaps(i) = itemVectors
    }
    for (rating <- sample) {
      val out = MF.learningRates.map(x => (x, rating))
      var k = 0
      for (i <- out) {
        val w: Vector = wMaps(k)(i._2.user)
        val h: Vector = hMaps(k)(i._2.item)
        (w, h) = sgdUpdaters(k).delta(i._2.rating, w, h, sample.size)
        wMaps(k).update(i._2.user, w)
        hMaps(k).update(i._2.item, h)
        k += 1
      }
    }
    stepSize.learningRate = getBestLearningRate(wMaps, hMaps, sample)
  }

  override def process(key: Int, context: Context, elements: Iterable[Rating],
                       out: Collector[Rating]): Unit = {
    userState = context.globalState.getMapState(userStateDescriptor)
    itemState = context.globalState.getMapState(itemStateDescriptor)
    testLearningRatesOnSample(elements)
    val ratings = Random.shuffle(elements.toList).iterator
    val strategy = Random.nextInt(2)
    var substrategies = 0 until MF.workerParallelism

    /* The number of subepochs is equal to the number of partitions of original matrix or, in other words, to the
       number of worker nodes. */
    for (i <- 0 until MF.workerParallelism) {
      val substrategy = Random.shuffle(substrategies.toList).head
      substrategies = substrategies.drop(1)
      val blocks = getStratum(strategy, substrategy)
      ratings.foreach(r => {
        for (i <- 0 until MF.workerParallelism) {
          if (r.userPartition == blocks._1(i) && r.itemPartition == blocks._2(i)) {
//            MF.stratumSize += 1
            out.collect(r)
            break
          }
        }
      })

      // TODO: wait
    }

      // Transform states into Scala maps in order to calculate the loss of this epoch.
//      val scalaUserMap: mutable.HashMap[UserId, Vector] = new mutable.HashMap[UserId, Vector]()
//      val scalaItemMap: mutable.HashMap[ItemId, Vector] = new mutable.HashMap[ItemId, Vector]()
//      userState.entries().forEach(kv => scalaUserMap.update(kv.getKey, kv.getValue))
//      itemState.entries().forEach(kv => scalaItemMap.update(kv.getKey, kv.getValue))
//      val epochLoss = getLoss(scalaUserMap, scalaItemMap, ratings)
  }

  override def open(parameters: Configuration): Unit = {
    userStateDescriptor = new MapStateDescriptor[UserId, Vector]("user-state", createTypeInformation[UserId],
      createTypeInformation[Vector])
    itemStateDescriptor = new MapStateDescriptor[ItemId, Vector]("item-state", createTypeInformation[ItemId],
      createTypeInformation[Vector])
  }
}

class SortSubstratums extends RichWindowFunction[(Rating, D), Rating, Int, TimeWindow] {
  private var count: ValueState[Int] = _

  override def apply(key: Int, window: TimeWindow, input: Iterable[(Rating, D)], out: Collector[Rating]): Unit = {
    val tmpCurrentCount = count.value()
    val currentCount = if (tmpCurrentCount != null) tmpCurrentCount else 0
    val newCount = currentCount + input.size
    count.update(newCount)
    Utils.testLearningRatesOnSample(input, MF.workerParallelism, MF.learningRates, MF.stepSize)
    input.toList.sortWith(_._2 < _._2).map(x => out.collect(x._1))
  }

  override def open(parameters: Configuration): Unit =
    count = getRuntimeContext.getState(new ValueStateDescriptor[Int]("count", createTypeInformation[Int]))

  override def close(): Unit = MF.n = count.value()
}
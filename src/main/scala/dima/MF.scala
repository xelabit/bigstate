package dima

import java.util.Properties

import dima.Utils._
import dima.InputTypes.Rating
import dima.ps.PSOnlineMatrixFactorization
import dima.ps.Vector._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector

class MF {

}

object MF {
  val numFactors = 10
  val rangeMin: Double = -0.1
  val rangeMax = 0.1
  val userMemory = 128
  val negativeSampleRate = 9
  val maxUId = 573
  val maxIId = 875
  val workerParallelism = 2
  val psParallelism = 2
  val learningRate = 0.01
  val pullLimit = 1500
  val iterationWaitTime = 10000

  def main(args: Array[String]): Unit = {
    val input_file_name = args(0)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)
    env.setMaxParallelism(2)

    // Kafka consumer
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
//    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test-consumer-group")
    val myConsumer = new FlinkKafkaConsumer011[String]("records", new SimpleStringSchema(), properties)
    myConsumer.setStartFromEarliest()
    val stream = env.addSource(myConsumer)
//    val data = env.readTextFile(input_file_name)
//    val formatter = DateTimeForm0at.forPattern("yyyy-MM-dd HH:mm:ss")
    val lastFM = stream.flatMap(new RichFlatMapFunction[String, (Rating, D)] {
//      private var x = 0

      override def flatMap(value: String, out: Collector[(Rating, D)]): Unit = {
        var distance = 0
        val fieldsArray = value.split(",")
        val uid = fieldsArray(1).toInt
        val iid = fieldsArray(2).toInt
        val userPartition = partitionId(uid, maxUId, workerParallelism)
        val itemPartition = partitionId(iid, maxIId, workerParallelism)
        val key = userPartition match {
          case 0 => 0
          case 1 => 2
        }
        val timestamp = fieldsArray(6).toLong
//        val timestamp = formatter.parseDateTime(fieldsArray(0)).getMillis
//        val timestamp = 1464882616000L + x
//        x += 1
        val r = Rating(key, uid, iid, fieldsArray(5).toInt, timestamp, userPartition, itemPartition, fieldsArray(3))
        if (r.userPartition != r.itemPartition) {
          distance = r.itemPartition - r.userPartition
          if (distance < 0) distance += MF.workerParallelism
        }
        out.collect((r, distance))
      }
    })
      .assignAscendingTimestamps(_._1.timestamp)
      .keyBy(_._1.key)
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
      .apply(new SortSubstratums)
    val losses = PSOnlineMatrixFactorization.psOnlineMF(lastFM, numFactors, rangeMin, rangeMax, learningRate,
      userMemory, negativeSampleRate, pullLimit, workerParallelism, psParallelism, iterationWaitTime, maxIId)
        .flatMap(new RichFlatMapFunction[Either[(String, W, Double), ((ItemId, Int), Vector)], (String, W, Double)] {

          override def flatMap(in: Either[(String, W, Double), ((ItemId, Int), Vector)],
                               collector: Collector[(String, W, Double)]): Unit = {
            in match {
              case Left((label, window, loss)) => collector.collect((label, window, loss))
              case _ => None
            }
          }
        })
        .keyBy(1)
        .sum(2)
        .map(x => x.toString)
//        .writeAsText("~/Documents/de/out")

    // Kafka Producer
    val myProducer = new FlinkKafkaProducer011[String]("localhost:9092","losses",
      new SimpleStringSchema)
    losses.addSink(myProducer)
//    val factorStream = PSOnlineMatrixFactorization.psOnlineMF(lastFM, numFactors, rangeMin, rangeMax, learningRate,
//      userMemory, negativeSampleRate, pullLimit, workerParallelism, psParallelism, iterationWaitTime, MF.maxIId)
//    lastFM
//      .connect(factorStream)
//      .flatMap(new RichCoFlatMapFunction[(Rating, W), Either[(W, UserId, Vector), (ItemId, Vector)], (Int, Double)] {
//        private var userVectors: MapState[UserId, Vector] = _
//        private var itemVectors: MapState[ItemId, Vector] = _
//        private var ratings: ListState[Rating] = _
//        private var ratingsBuffer: ListState[Rating] = _
//        private var epoch: ValueState[Int] = _
//        private var windowId: ValueState[Long] = _
//
//        override def flatMap1(in1: (Rating, W), collector: Collector[(Int, Double)]): Unit = {
//          if (in1._2 == windowId.value() | windowId == null) ratings.add(in1._1)
//          else ratingsBuffer.add(in1._1)
//        }
//
//        override def flatMap2(in2: Either[(W, Double), (ItemId, Vector)], collector: Collector[(Int, Double)]
//                             ): Unit = {
//          val tmpWindowId = windowId.value()
//          val tmpEpoch = epoch.value()
//          val currentEpoch = if (tmpEpoch != null) tmpEpoch else 0
//          in2 match {
//            case Left((w, userId, vec)) => {
//              val currentWindowId = if (tmpWindowId != null) tmpWindowId else w
//              if (w == currentWindowId) userVectors.put(userId, vec)
//              else {
//                windowId.update(w)
//                val scalaRatings = ratings.get().iterator()
//                val epochLoss = Utils.getLoss(userVectors, itemVectors, scalaRatings)
//                collector.collect(currentEpoch, epochLoss)
//                epoch.update(currentEpoch + 1)
//                ratings.clear()
//                ratings = ratingsBuffer
//                ratingsBuffer.clear()
//                userVectors.put(userId, vec)
//              }
//            }
//            case Right((itemId, vec)) => itemVectors.put(itemId, vec)
//          }
//        }
//
//        override def open(parameters: Configuration): Unit = {
//          userVectors = getRuntimeContext.getMapState(new MapStateDescriptor[UserId, Vector]("users",
//            createTypeInformation[UserId], createTypeInformation[Vector]))
//          itemVectors = getRuntimeContext.getMapState(new MapStateDescriptor[ItemId, Vector]("items",
//            createTypeInformation[ItemId], createTypeInformation[Vector]))
//          ratings = getRuntimeContext.getListState(
//            new ListStateDescriptor[Rating]("ratings", createTypeInformation[Rating])
//          )
//          ratings = getRuntimeContext.getListState(
//            new ListStateDescriptor[Rating]("temp-ratings", createTypeInformation[Rating])
//          )
//          epoch = getRuntimeContext.getState(
//            new ValueStateDescriptor[Int]("loss", createTypeInformation[Int])
//          )
//          windowId = getRuntimeContext.getState(
//            new ValueStateDescriptor[Long]("window-id", createTypeInformation[Long])
//          )
//        }
//      })
//      .keyBy(_._1)
//      .sum(1)
//      .print()
    env.execute()
  }
}

class SortSubstratums extends RichWindowFunction[(Rating, D), (Rating, W), Int, TimeWindow] {

  override def apply(key: Int, window: TimeWindow, input: Iterable[(Rating, D)], out: Collector[(Rating, W)]): Unit =
    input.toList.sortWith(_._2 < _._2).map(x => out.collect(x._1, window.getStart))
}

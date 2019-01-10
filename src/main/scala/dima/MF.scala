package dima

import java.util.Properties

import dima.Utils._
import dima.InputTypes.Rating
import dima.ps.PSOnlineMatrixFactorization
import dima.ps.Vector._

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.math.max

class MF {

}

object MF {
  val numFactors = 25
  val rangeMin: Double = -0.1
  val rangeMax = 0.1
  val userMemory = 128
  val negativeSampleRate = 9
  val maxUId = 521684
  val maxIId = 2299712
  val workerParallelism = 18
  val psParallelism = 18
  val learningRate = 0.03
  val pullLimit = 1500
  val iterationWaitTime = 10000
  private val log = LoggerFactory.getLogger(classOf[MF])

  def main(args: Array[String]): Unit = {
    val input_file_name = args(0)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(18)
    env.setMaxParallelism(4096)

    // Kafka consumer
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "ibm-power-3.dima.tu-berlin.de:9092")
//    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test-consumer-group")
    val myConsumer = new FlinkKafkaConsumer011[String]("ratings", new SimpleStringSchema(), properties)
    myConsumer.setStartFromEarliest()
    val stream = env.addSource(myConsumer).setParallelism(2)
//    val data = env.readTextFile(input_file_name)
//    val formatter = DateTimeForm0at.forPattern("yyyy-MM-dd HH:mm:ss")
    val lastFM = stream.flatMap(new RichFlatMapFunction[String, (Rating, D)] {
//      private var x = 0

      override def flatMap(value: String, out: Collector[(Rating, D)]): Unit = {
        var distance = 0
        val fieldsArray = value.split(",")
        val uid = fieldsArray(1).toInt
        val ingestion = System.currentTimeMillis()
        val iid = fieldsArray(2).toInt
        val userPartition = partitionId(uid, maxUId, workerParallelism)
        val itemPartition = partitionId(iid, maxIId, workerParallelism)
        if (userPartition == -1) println(s"uid $uid")
        if (itemPartition == -1) println(s"iid $iid")
        val key = userPartition match {
          case 1 => 155
          case 2 => 291
          case 3 => 160
          case 4 => 205
          case 5 => 528
          case 6 => 55
          case 7 => 67
          case 8 => 395
          case 9 => 568
          case 10 => 95
          case 11 => 7
          case 12 => 4
          case 13 => 32
          case 14 => 236
          case 15 => 693
          case 16 => 57
          case 17 => 82
          case 18 => 52
          case 19 => 113
          case 20 => 159
          case 21 => 37
          case 22 => 163
          case 23 => 19
          case 24 => 171
          case 25 => 543
          case 26 => 10
          case 27 => 380
          case 28 => 16
          case 29 => 28
          case 30 => 172
          case 31 => 277
          case 32 => 241
          case 33 => 650
          case 34 => 88
          case 35 => 143
          case 36 => 73
          case 37 => 144
          case 38 => 505
          case 39 => 60
          case 40 => 381
          case 41 => 36
          case 42 => 22
          case 43 => 535
          case 44 => 146
          case 45 => 180
          case 46 => 365
          case 47 => 777
          case 48 => 12
          case 49 => 108
          case 50 => 607
          case 51 => 287
          case 52 => 92
          case 53 => 72
          case 54 => 122
          case 55 => 90
          case 56 => 35
          case 57 => 457
          case 58 => 371
          case 59 => 566
          case 60 => 53
          case 61 => 190
          case 62 => 26
          case 63 => 653
          case 64 => 294
          case 65 => 273
          case 66 => 232
          case 67 => 138
          case 68 => 71
          case 69 => 102
          case 70 => 38
          case 71 => 508
          case 72 => 64
          case 73 => 121
          case 74 => 137
          case 75 => 33
          case 76 => 167
          case 77 => 15
          case 78 => 206
          case 79 => 34
          case 80 => 134
          case 81 => 202
          case 82 => 592
          case 83 => 213
          case 84 => 375
          case 85 => 58
          case 86 => 50
          case 87 => 133
          case 88 => 192
          case 89 => 11
          case 90 => 147
          case 91 => 315
          case 92 => 14
          case 93 => 94
          case 94 => 21
          case 95 => 179
          case 96 => 328
          case 97 => 30
          case 98 => 9
          case 99 => 41
          case 100 => 459
          case 101 => 289
          case 102 => 244
          case 103 => 227
          case 104 => 74
          case 105 => 109
          case 106 => 23
          case 107 => 3
          case 108 => 456
          case 109 => 51
          case 110 => 714
          case 111 => 27
          case 112 => 0
          case 113 => 39
          case 114 => 66
          case 115 => 211
          case 116 => 364
          case 117 => 59
          case 118 => 31
          case 119 => 69
          case 120 => 29
          case 121 => 218
          case 122 => 56
          case 123 => 449
          case 124 => 193
          case 125 => 8
          case 126 => 270
          case 127 => 76
          case 128 => 142
          case 129 => 710
          case 130 => 2
          case 131 => 40
          case 132 => 77
          case 133 => 17
          case 134 => 48
          case 135 => 68
          case 136 => 307
          case 137 => 24
          case 138 => 148
          case 139 => 45
          case 140 => 349
          case 141 => 81
          case 142 => 6
          case 143 => 25
          case 144 => 78
          case 145 => 18
          case 146 => 164
          case 147 => 44
          case 148 => 111
          case 149 => 89
          case 150 => 274
          case 151 => 106
          case 152 => 516
          case 153 => 54
          case 154 => 132
          case 155 => 43
          case 156 => 260
          case 157 => 314
          case 158 => 209
          case 159 => 42
          case 160 => 63
          case 161 => 112
          case 162 => 126
          case 163 => 156
          case 164 => 197
          case 165 => 214
          case 166 => 49
          case 167 => 115
          case 168 => 141
          case 169 => 150
          case 170 => 104
          case 171 => 379   
          case 172 => 326
          case 173 => 83
          case 174 => 79
          case 175 => 196
          case 176 => 93
          case 177 => 131
          case 178 => 47
          case 179 => 1
          case 180 => 377
        }
        val timestamp = fieldsArray(6).toLong
//        val timestamp = formatter.parseDateTime(fieldsArray(0)).getMillis
//        val timestamp = 1464882616000L + x
//        x += 1
        val r = Rating(key, uid, iid, fieldsArray(4).toInt, timestamp, userPartition, itemPartition, fieldsArray(3),
                       ingestion)
        if (r.userPartition != r.itemPartition) {
          distance = r.itemPartition - r.userPartition
          if (distance < 0) distance += MF.workerParallelism
        }
        out.collect((r, distance))
      }
    })
      //.assignAscendingTimestamps(_._1.timestamp)
     // .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Rating, D)](Time.seconds(10)) {

       // override def extractTimestamp(event: (Rating, D)): Long = event._1.timestamp
     // })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator) 
      .keyBy(_._1.key)
      .window(TumblingEventTimeWindows.of(Time.hours(12)))
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
    val producerProperties = new Properties()
    producerProperties.setProperty("bootstrap.servers", "ibm-power-3.dima.tu-berlin.de:9092")
    producerProperties.setProperty("request.timeout.ms", "300000") //default 30
    val myProducer = new FlinkKafkaProducer011[String]("lossesibm3", new SimpleStringSchema, producerProperties)
    losses.addSink(myProducer).setParallelism(2)
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

  override def apply(key: Int, window: TimeWindow, input: Iterable[(Rating, D)], out: Collector[(Rating, W)]): Unit = {
    val lastRecord = input.last
    val latency = System.currentTimeMillis() - lastRecord._1.ingestionTime
    println(s"Latency: $latency")
    input.toList.sortWith(_._2 < _._2).map(x => out.collect(x._1, window.getStart))
  }
}

class BoundedOutOfOrdernessGenerator extends AssignerWithPeriodicWatermarks[(Rating, D)] {
  val maxOutOfOrderness = 3500L
  var currentMaxTimestamp: Long = _

  override def extractTimestamp(element: (Rating, D), previousElementTimestamp: Long): Long = {
    val timestamp = element._1.timestamp
    currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
    timestamp
  }

  override def getCurrentWatermark(): Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }
}

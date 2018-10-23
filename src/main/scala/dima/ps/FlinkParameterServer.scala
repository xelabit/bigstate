package dima.ps

import dima.Utils.ItemId
import dima.ps.receiver._
import dima.ps.sender._
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class FlinkParameterServer

object FlinkParameterServer {
  private val log = LoggerFactory.getLogger(classOf[FlinkParameterServer])

  def transform[T, Id, P, PSOut, WOut](trainingData: DataStream[T], workerLogic: WorkerLogic[T, Id, P, WOut],
                                       psLogic: ParameterServerLogic[Id, P, PSOut], workerParallelism: Int,
                                       psParallelism: Int,
                                       iterationWaitTime: Long)(implicit tiT: TypeInformation[T],
                                                                tiId: TypeInformation[Id], tiP: TypeInformation[P],
                                                                tiPSOut: TypeInformation[PSOut],
                                                                tiWOut: TypeInformation[WOut]
                                                               ): DataStream[Either[WOut, PSOut]] = {
    val hashFunc: Id => Int = x => x.asInstanceOf[(ItemId, Int)]._2

    /* The partiton of item factor vectors should be happening in another way, i.e. according to the itemPartition field
       of a Rating tuple. */
    val workerToPSPartitioner: WorkerToPS[Id, P] => Int = {
      case WorkerToPS(_, msg) =>
        msg match {
          case Left(Pull(pId)) => hashFunc(pId)
          case Right(Push(pId, _)) => hashFunc(pId)
        }
    }
    val psToWorkerPartitioner: PSToWorker[Id, P] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }
    transform1[T, Id, P, PSOut, WOut, PSToWorker[Id, P], WorkerToPS[Id, P]](trainingData, workerLogic, psLogic,
      workerToPSPartitioner, psToWorkerPartitioner, workerParallelism, psParallelism, new SimpleWorkerReceiver[Id, P],
      new SimpleWorkerSender[Id, P], new SimplePSReceiver[Id, P], new SimplePSSender[Id, P], iterationWaitTime)
  }

  def transform1[T, Id, P, PSOut, WOut, PStoWorker, WorkerToPS](trainingData: DataStream[T],
                                                                workerLogic: WorkerLogic[T, Id, P, WOut],
                                                                psLogic: ParameterServerLogic[Id, P, PSOut],
                                                                paramPartitioner: WorkerToPS => Int,
                                                                wInPartition: PStoWorker => Int, workerParallelism: Int,
                                                                psParallelism: Int,
                                                                workerReceiver: WorkerReceiver[PStoWorker, Id, P],
                                                                workerSender: WorkerSender[WorkerToPS, Id, P],
                                                                psReceiver: PSReceiver[WorkerToPS, Id, P],
                                                                psSender: PSSender[PStoWorker, Id, P],
                                                                iterationWaitTime: Long)
                                                               (implicit tiT: TypeInformation[T],
                                                                tiId: TypeInformation[Id],
                                                                tiP: TypeInformation[P],
                                                                tiPSOut: TypeInformation[PSOut],
                                                                tiWOut: TypeInformation[WOut],
                                                                tiWorkerIn: TypeInformation[PStoWorker],
                                                                tiWorkerOut: TypeInformation[WorkerToPS]
                                                               ): DataStream[Either[WOut, PSOut]] = {
    def stepFunc(workerIn: ConnectedStreams[T, PStoWorker]
                ): (DataStream[PStoWorker], DataStream[Either[WOut, PSOut]]) = {
      val worker = workerIn
        .flatMap(
          new RichCoFlatMapFunction[T, PStoWorker, Either[WorkerToPS, WOut]] {
            val receiver: WorkerReceiver[PStoWorker, Id, P] = workerReceiver
            val sender: WorkerSender[WorkerToPS, Id, P] = workerSender
            val logic: WorkerLogic[T, Id, P, WOut] = workerLogic
            val psClient = new MessagingPSClient[PStoWorker, WorkerToPS, Id, P, WOut](sender)

            override def open(parameters: Configuration): Unit = {
              logic.open()
              psClient.setPartitionId(getRuntimeContext.getIndexOfThisSubtask)
            }

            // incoming answer from PS
            override def flatMap2(msg: PStoWorker, out: Collector[Either[WorkerToPS, WOut]]): Unit = {
              log.debug(s"Pull answer: $msg")
              psClient.setCollector(out)
              receiver.onPullAnswerRecv(msg, {
                case PullAnswer(id, value) => logic.onPullRecv(id, value, psClient)
              })
            }

            // incoming data
            override def flatMap1(data: T, out: Collector[Either[WorkerToPS, WOut]]): Unit = {
              log.debug(s"Incoming data: $data")
              psClient.setCollector(out)
              logic.onRecv(data, psClient)
            }

            override def close(): Unit = {
              logic.close()
            }
          }
        )
        .setParallelism(workerParallelism)
      val wOut = worker.flatMap(x => x match {
        case Right(out) => Some(out)
        case _ => None
      }).setParallelism(workerParallelism)
      val ps = worker
        .flatMap(x => x match {
          case Left(workerOut) => Some(workerOut)
          case _ => None
        }).setParallelism(workerParallelism)
        .partitionCustom(new Partitioner[Int]() {

          override def partition(key: Int, numPartitions: Int): Int = key % numPartitions
        }, paramPartitioner)
        .flatMap(new RichFlatMapFunction[WorkerToPS, Either[PStoWorker, PSOut]] {
          val logic: ParameterServerLogic[Id, P, PSOut] = psLogic
          val receiver: PSReceiver[WorkerToPS, Id, P] = psReceiver
          val sender: PSSender[PStoWorker, Id, P] = psSender
          val ps = new MessagingPS[PStoWorker, WorkerToPS, Id, P, PSOut](sender)

          override def flatMap(msg: WorkerToPS, out: Collector[Either[PStoWorker, PSOut]]): Unit = {
            log.debug(s"Pull request or push msg @ PS: $msg")
            ps.setCollector(out)
            receiver.onWorkerMsg(msg,
              (pullId, workerPartitionIndex) => logic.onPullRecv(pullId, workerPartitionIndex, ps),
              { case (pushId, deltaUpdate) => logic.onPushRecv(pushId, deltaUpdate, ps) }
            )
          }

          override def close(): Unit = {
            logic.close(ps)
          }

          override def open(parameters: Configuration): Unit =
            logic.open(parameters: Configuration, getRuntimeContext: RuntimeContext)
        })
        .setParallelism(psParallelism)
      val psToWorker = ps
        .flatMap(_ match {
          case Left(x) => Some(x)
          case _ => None
        })
        .setParallelism(psParallelism)

        // TODO avoid this empty map?
        .map(x => x).setParallelism(workerParallelism)
        .partitionCustom(new Partitioner[Int]() {

          override def partition(key: Int, numPartitions: Int): Int = {
            if (0 <= key && key < numPartitions) key
            else throw new RuntimeException("Pull answer key should be the partition ID itself!")
          }
        }, wInPartition)
      val psToOut = ps.flatMap(_ match {
        case Right(x) => Some(x)
        case _ => None
      }).setParallelism(psParallelism)
      val wOutEither: DataStream[Either[WOut, PSOut]] = wOut.forward.map(x => Left(x))
      val psOutEither: DataStream[Either[WOut, PSOut]] = psToOut.forward.map(x => Right(x))
      (psToWorker, wOutEither.setParallelism(workerParallelism).union(psOutEither.setParallelism(psParallelism)))
    }
    trainingData
      .map(x => x)
      .setParallelism(workerParallelism)
      .iterate((x: ConnectedStreams[T, PStoWorker]) => stepFunc(x), iterationWaitTime)
  }

  private class MessagingPS[WorkerIn, WorkerOut, Id, P, PSOut](psSender: PSSender[WorkerIn, Id, P])
    extends ParameterServer[Id, P, PSOut] {

    private var collector: Collector[Either[WorkerIn, PSOut]] = _

    def setCollector(out: Collector[Either[WorkerIn, PSOut]]): Unit = collector = out

    def collectAnswerMsg(msg: WorkerIn): Unit = collector.collect(Left(msg))

    override def answerPull(id: Id, value: P, workerPartitionIndex: Int): Unit = {
      psSender.onPullAnswer(id, value, workerPartitionIndex, collectAnswerMsg)
    }

    override def output(out: PSOut): Unit = collector.collect(Right(out))
  }

  private class MessagingPSClient[IN, OUT, Id, P, WOut](sender: WorkerSender[OUT, Id, P])
    extends ParameterServerClient[Id, P, WOut] {
    private var collector: Collector[Either[OUT, WOut]] = _
    private var partitionId: Int = -1

    def setPartitionId(pId: Int): Unit = partitionId = pId

    def setCollector(out: Collector[Either[OUT, WOut]]): Unit = collector = out

    def collectPullMsg(msg: OUT): Unit = collector.collect(Left(msg))

    override def pull(id: Id): Unit = sender.onPull(id, collectPullMsg, partitionId)

    override def push(id: Id, deltaUpdate: P): Unit = sender.onPush(id, deltaUpdate, collectPullMsg, partitionId)

    override def output(out: WOut): Unit = collector.collect(Right(out))
  }
}

trait ParameterServerLogic[Id, P, PSOut] extends Serializable {

  def onPullRecv(id: Id, workerPartitionIndex: Int, ps: ParameterServer[Id, P, PSOut]): Unit

  def onPushRecv(id: Id, deltaUpdate: P, ps: ParameterServer[Id, P, PSOut]): Unit

  def close(ps: ParameterServer[Id, P, PSOut]): Unit = ()

  def open(parameters: Configuration, runtimeContext: RuntimeContext): Unit = ()
}

trait ParameterServer[Id, P, PSOut] extends Serializable {

  def answerPull(id: Id, value: P, workerPartitionIndex: Int): Unit

  def output(out: PSOut): Unit
}

trait PSReceiver[WorkerToPS, Id, P] extends Serializable {

  def onWorkerMsg(msg: WorkerToPS, onPullRecv: (Id, Int) => Unit, onPushRecv: (Id, P) => Unit)
}

trait PSSender[PStoWorker, Id, P] extends Serializable {

  def onPullAnswer(id: Id, value: P, workerPartitionIndex: Int, collectAnswerMsg: PStoWorker => Unit)
}

trait WorkerReceiver[PStoWorker, Id, P] extends Serializable {

  def onPullAnswerRecv(msg: PStoWorker, pullHandler: PullAnswer[Id, P] => Unit)
}

trait WorkerSender[WorkerToPS, Id, P] extends Serializable {

  def onPull(id: Id, collectAnswerMsg: WorkerToPS => Unit, partitionId: Int)

  def onPush(id: Id, deltaUpdate: P, collectAnswerMsg: WorkerToPS => Unit, partitionId: Int)
}
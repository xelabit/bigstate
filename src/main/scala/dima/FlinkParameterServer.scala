package dima

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream

class FlinkParameterServer

object FlinkParameterServer {
  def transform[T, P, PSOut, WOut](trainingData: DataStream[T], workerLogic: WorkerLogic[T, P, WOut],
                                   psLogic: ParameterServerLogic[P, PSOut], workerParallelism: Int, psParallelism: Int,
                                   iterationWaitTime: Long)(implicit tiT: TypeInformation[T], tiP: TypeInformation[P],
                                   tiPSOut: TypeInformation[PSOut], tiWOut: TypeInformation[WOut]):
  DataStream[Either[WOut, PSOut]] = {
    val hashFunc: Any => Int = x => Math.abs(x.hashCode())
    val workerToPSPartitioner: WorkerToPS[P] => Int = {
      case WorkerToPS(_, msg) =>
        msg match {
          case Left(Pull(pId)) => hashFunc(pId) % psParallelism
          case Right(Push(pId, _)) => hashFunc(pId) % psParallelism
        }
    }
    val psToWorkerPartitioner: PSToWorker[P] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }
    transform[T, P, PSOut, WOut, PSToWorker[P], WorkerToPS[P]](trainingData, workerLogic, psLogic, workerToPSPartitioner,
      psToWorkerPartitioner, workerParallelism, psParallelism, new SimpleWorkerReceiver[P], new SimpleWorkerSender[P],
      new SimplePSReceiver[P], new SimplePSSender[P], iterationWaitTime)
  }

  def transform[T, P, PSOut, WOut, PStoWorker, WorkerToPS](trainingData: DataStream[T],
                                                           workerLogic: WorkerLogic[T, P, WOut],
                                                           psLogic: ParameterServerLogic[P, PSOut],
                                                           paramPartitioner: WorkerToPS => Int,
                                                           wInPartition: PStoWorker => Int,
                                                           workerParallelism: Int,
                                                           psParallelism: Int,
                                                           workerReceiver: WorkerReceiver[PStoWorker, P],
                                                           workerSender: WorkerSender[WorkerToPS, P],
                                                           psReceiver: PSReceiver[WorkerToPS, P],
                                                           psSender: PSSender[PStoWorker, P],
                                                           iterationWaitTime: Long)
                                                          (implicit
                                                           tiT: TypeInformation[T],
                                                           tiP: TypeInformation[P],
                                                           tiPSOut: TypeInformation[PSOut],
                                                           tiWOut: TypeInformation[WOut],
                                                           tiWorkerIn: TypeInformation[PStoWorker],
                                                           tiWorkerOut: TypeInformation[WorkerToPS]
                                                          ): DataStream[Either[WOut, PSOut]] = {
    def stepFunc(workerIn: ConnectedStreams[T, PStoWorker]):
    (DataStream[PStoWorker], DataStream[Either[WOut, PSOut]]) = {

      val worker = workerIn
        .flatMap(
          new RichCoFlatMapFunction[T, PStoWorker, Either[WorkerToPS, WOut]] {

            val receiver: WorkerReceiver[PStoWorker, P] = workerReceiver
            val sender: WorkerSender[WorkerToPS, P] = workerSender
            val logic: WorkerLogic[T, P, WOut] = workerLogic

            val psClient =
              new MessagingPSClient[PStoWorker, WorkerToPS, P, WOut](sender)


            override def open(parameters: Configuration): Unit = {
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
          override def partition(key: Int, numPartitions: Int): Int = {
            key % numPartitions
          }
        }, paramPartitioner)
        .flatMap(new RichFlatMapFunction[WorkerToPS, Either[PStoWorker, PSOut]] {

          val logic: ParameterServerLogic[P, PSOut] = psLogic
          val receiver: PSReceiver[WorkerToPS, P] = psReceiver
          val sender: PSSender[PStoWorker, P] = psSender

          val ps = new MessagingPS[PStoWorker, WorkerToPS, P, PSOut](sender)

          override def flatMap(msg: WorkerToPS, out: Collector[Either[PStoWorker, PSOut]]): Unit = {
            log.debug(s"Pull request or push msg @ PS: $msg")

            ps.setCollector(out)
            receiver.onWorkerMsg(msg,
              (pullId, workerPartitionIndex) => logic.onPullRecv(pullId, workerPartitionIndex, ps), { case (pushId, deltaUpdate) => logic.onPushRecv(pushId, deltaUpdate, ps) }
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
            if (0 <= key && key < numPartitions) {
              key
            } else {
              throw new RuntimeException("Pull answer key should be the partition ID itself!")
            }
          }
        }, wInPartition)

      val psToOut = ps.flatMap(_ match {
        case Right(x) => Some(x)
        case _ => None
      })
        .setParallelism(psParallelism)

      val wOutEither: DataStream[Either[WOut, PSOut]] = wOut.forward.map(x => Left(x))
      val psOutEither: DataStream[Either[WOut, PSOut]] = psToOut.forward.map(x => Right(x))

      (psToWorker, wOutEither.setParallelism(workerParallelism).union(psOutEither.setParallelism(psParallelism)))
    }

    trainingData
      .map(x => x)
      .setParallelism(workerParallelism)
      .iterate((x: ConnectedStreams[T, PStoWorker]) => stepFunc(x), iterationWaitTime)
  }
}

trait ParameterServerLogic[P, PSOut] extends Serializable {

  def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, PSOut]): Unit

  def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, PSOut]): Unit

  def close(ps: ParameterServer[P, PSOut]): Unit = ()

  def open(parameters: Configuration, runtimeContext: RuntimeContext): Unit = ()
}

trait ParameterServer[P, PSOut] extends Serializable {

  def answerPull(id: Int, value: P, workerPartitionIndex: Int): Unit

  def output(out: PSOut): Unit
}
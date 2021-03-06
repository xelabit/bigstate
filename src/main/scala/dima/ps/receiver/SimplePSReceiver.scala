package dima.ps.receiver

import dima.ps.{PSReceiver, Pull, Push, WorkerToPS}

class SimplePSReceiver[Id, P] extends PSReceiver[WorkerToPS[Id, P], Id, P] {

  override def onWorkerMsg(wToPS: WorkerToPS[Id, P], onPullRecv: (Id, Int) => Unit, onPushRecv: (Id, P) => Unit): Unit = {
    wToPS.msg match {
      case Left(Pull(paramId)) =>

        // Passes key and partitionID
        onPullRecv(paramId, wToPS.workerPartitionIndex)
      case Right(Push(paramId, delta)) => onPushRecv(paramId, delta)
      case _ => throw new Exception("Parameter server received unknown message.")
    }
  }
}

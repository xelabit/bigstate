package dima.ps.sender

import dima.ps.{Pull, Push, WorkerSender, WorkerToPS}

class SimpleWorkerSender[P] extends WorkerSender[WorkerToPS[P], P] {

  override def onPull(id: Int, collectAnswerMsg: WorkerToPS[P] => Unit, partitionId: Int): Unit =
    collectAnswerMsg(WorkerToPS(partitionId, Left(Pull(id))))

  override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerToPS[P] => Unit, partitionId: Int): Unit =
    collectAnswerMsg(WorkerToPS(partitionId, Right(Push(id, deltaUpdate))))
}

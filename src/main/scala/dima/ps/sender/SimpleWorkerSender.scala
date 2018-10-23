package dima.ps.sender

import dima.ps.{Pull, Push, WorkerSender, WorkerToPS}

class SimpleWorkerSender[Id, P] extends WorkerSender[WorkerToPS[Id, P], Id, P] {

  override def onPull(id: Id, collectAnswerMsg: WorkerToPS[Id, P] => Unit, partitionId: Int): Unit =
    collectAnswerMsg(WorkerToPS(partitionId, Left(Pull(id))))

  override def onPush(id: Id, deltaUpdate: P, collectAnswerMsg: WorkerToPS[Id, P] => Unit, partitionId: Int): Unit =
    collectAnswerMsg(WorkerToPS(partitionId, Right(Push(id, deltaUpdate))))
}

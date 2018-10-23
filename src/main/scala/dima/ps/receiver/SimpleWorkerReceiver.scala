package dima.ps.receiver

import dima.ps.{PSToWorker, PullAnswer, WorkerReceiver}

class SimpleWorkerReceiver[Id, P] extends WorkerReceiver[PSToWorker[Id, P], Id, P] {

  override def onPullAnswerRecv(msg: PSToWorker[Id, P], pullHandler: PullAnswer[Id, P] => Unit): Unit =
    msg match { case PSToWorker(_, pullAns) => pullHandler(pullAns) }
}

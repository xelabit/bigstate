package dima

class SimpleWorkerReceiver[P] extends WorkerReceiver[PSToWorker[P], P] {

  override def onPullAnswerRecv(msg: PSToWorker[P], pullHandler: PullAnswer[P] => Unit): Unit =
    msg match { case PSToWorker(_, pullAns) => pullHandler(pullAns) }
}

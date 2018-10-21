package dima.ps.sender

import dima.ps.{PSSender, PSToWorker, PullAnswer}

class SimplePSSender[P] extends PSSender[PSToWorker[P], P] {

  override def onPullAnswer(id: Int, value: P, workerPartitionIndex: Int, collectAnswerMsg: (PSToWorker[P]) => Unit
                           ): Unit = collectAnswerMsg(PSToWorker[P](workerPartitionIndex, PullAnswer(id, value)))
}

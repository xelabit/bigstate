package dima.ps

import scala.collection.mutable

trait WorkerLogic[T, P, WOut] extends Serializable {

  def open(): Unit = {}

  def onRecv(data: T, ps: ParameterServerClient[P, WOut]): Unit

  def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit

  def close(): Unit = ()
}

object WorkerLogic {
  def addPullLimiter[T, P, WOut](workerLogic: WorkerLogic[T, P, WOut], pullLimit: Int): WorkerLogic[T, P, WOut] = {
    new WorkerLogic[T, P, WOut] {
      private var pullCounter = 0
      private val pullQueue = mutable.Queue[Int]()
      val wrappedPS = new ParameterServerClient[P, WOut] {
        private var ps: ParameterServerClient[P, WOut] = _
        
        def setPS(ps: ParameterServerClient[P, WOut]): Unit = {
          this.ps = ps
        }

        override def pull(id: Int): Unit = {
          if (pullCounter < pullLimit) {
            pullCounter += 1
            ps.pull(id)
          } else {
            pullQueue.enqueue(id)
          }
        }

        override def push(id: Int, deltaUpdate: P): Unit = {
          ps.push(id, deltaUpdate)
        }

        override def output(out: WOut): Unit = {
          ps.output(out)
        }
      }

      override def onRecv(data: T, ps: ParameterServerClient[P, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onRecv(data, wrappedPS)
      }

      override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onPullRecv(paramId, paramValue, wrappedPS)
        pullCounter -= 1
        if (pullQueue.nonEmpty) {
          wrappedPS.pull(pullQueue.dequeue())
        }
      }
    }
  }
}
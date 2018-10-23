package dima.ps

import scala.collection.mutable

trait WorkerLogic[T, Id, P, WOut] extends Serializable {

  def open(): Unit = {}

  def onRecv(data: T, ps: ParameterServerClient[Id, P, WOut]): Unit

  def onPullRecv(paramId: Id, paramValue: P, ps: ParameterServerClient[Id, P, WOut]): Unit

  def close(): Unit = ()
}

object WorkerLogic {
  def addPullLimiter[T, Id, P, WOut](workerLogic: WorkerLogic[T, Id, P, WOut], pullLimit: Int
                                    ): WorkerLogic[T, Id, P, WOut] = {
    new WorkerLogic[T, Id, P, WOut] {
      private var pullCounter = 0
      private val pullQueue = mutable.Queue[Id]()
      val wrappedPS = new ParameterServerClient[Id, P, WOut] {
        private var ps: ParameterServerClient[Id, P, WOut] = _
        
        def setPS(ps: ParameterServerClient[Id, P, WOut]): Unit = {
          this.ps = ps
        }

        override def pull(id: Id): Unit = {
          if (pullCounter < pullLimit) {
            pullCounter += 1
            ps.pull(id)
          } else {
            pullQueue.enqueue(id)
          }
        }

        override def push(id: Id, deltaUpdate: P): Unit = {
          ps.push(id, deltaUpdate)
        }

        override def output(out: WOut): Unit = {
          ps.output(out)
        }
      }

      override def onRecv(data: T, ps: ParameterServerClient[Id, P, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onRecv(data, wrappedPS)
      }

      override def onPullRecv(paramId: Id, paramValue: P, ps: ParameterServerClient[Id, P, WOut]): Unit = {
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
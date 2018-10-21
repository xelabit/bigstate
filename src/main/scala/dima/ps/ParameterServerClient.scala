package dima.ps

trait ParameterServerClient[P, WorkerOut] extends Serializable {

  def pull(id: Int): Unit

  def push(id: Int, deltaUpdate: P): Unit

  def output(out: WorkerOut): Unit
}

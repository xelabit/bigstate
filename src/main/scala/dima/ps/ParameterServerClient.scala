package dima.ps

trait ParameterServerClient[Id, P, WorkerOut] extends Serializable {

  def pull(id: Id): Unit

  def push(id: Id, deltaUpdate: P): Unit

  def output(out: WorkerOut): Unit
}

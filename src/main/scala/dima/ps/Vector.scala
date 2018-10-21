package dima.ps

object Vector {
  type Vector = Array[Double]

  def vectorSum(u: Vector, v: Vector): Array[Double] = {
    val n = u.length
    val res = new Array[Double](n)
    var i = 0
    while (i < n) {
      res(i) = u(i) + v(i)
      if (res(i).isNaN) {
        throw new FactorIsNotANumberException
      }
      i += 1
    }
    res
  }

  class FactorIsNotANumberException extends Exception
}

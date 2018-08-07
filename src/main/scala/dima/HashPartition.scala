def partitionId(id: Int, maxId: Int, n: Int): Int = {
  val partitionSize = maxId / n
  val leftover = maxId - partitionSize * n
  val flag = (leftover == 0)
  if (id < maxId) getHash(id, 0, flag, partitionSize, leftover)
  else -1
}

def getHash(x: Int, partition: Int, flag: Boolean, partitionSize: Int, leftover: Int): Int = {
  var f = flag
  var p = partition
  var ps = partitionSize
  var l = leftover
  if (f == true) {
    ps += 1
    l -= 1
  }
  if (l > 0) f = true
  if (x <= ps) p
  else {
    ps -= 1
    ps += ps
    p += 1
    getHash(x, p, f, ps, l)
  }
}

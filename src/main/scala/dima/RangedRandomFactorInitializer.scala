package dima

import scala.util.Random

class RangedRandomFactorInitializer(random: Random, numFactors: Int, rangeMin: Double, rangeMax: Double) {
  def nextFactor(id: Int): Array[Double] = {
    Array.fill(numFactors)(rangeMin + (rangeMax - rangeMin) * random.nextDouble)
  }
}

case class RangedRandomFactorInitializerDescriptor(numFactors: Int, rangeMin: Double, rangeMax: Double) {
  def open(): RangedRandomFactorInitializer =
    new RangedRandomFactorInitializer(scala.util.Random, numFactors, rangeMin, rangeMax)
}

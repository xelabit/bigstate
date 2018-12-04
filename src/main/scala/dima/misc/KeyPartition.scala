package dima.misc

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.util.{Collector, MathUtils}

object KeyPartition {

  def main(args: Array[String]): Unit = {
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    var executionConfig = streamingEnv.getConfig
    val p = 2
    val maxP = 4096
    executionConfig.setParallelism(p)
    executionConfig.setMaxParallelism(maxP)
    val k = 0 to 100 toList
    val v = 20 to 120 toList
    val elements = k zip v
    val data = streamingEnv.fromCollection(elements)
    val keys = data.keyBy(0).flatMap(new RichFlatMapFunction[(Int, Int), (Int, Int)] {

      override def flatMap(in: (Int, Int), collector: Collector[(Int, Int)]): Unit = {
        val result = (in._1.hashCode(), MathUtils.murmurHash(in._1.hashCode()) % maxP * p / maxP)
        collector.collect(result)
      }
    })
      .print()
    streamingEnv.execute("KeyPartition")
  }
}

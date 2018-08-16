/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dima

import java.lang
import java.util.Map

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.state.MapState
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.ml.math.{DenseMatrix, DenseVector}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.io.TupleCsvInputFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.function.{RichWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your appliation into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  type OUT = (DateTime, Int, Int, Int)
  val Rank = 100

  def main(args: Array[String]) {
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
    val data = streamingEnv.readTextFile("~/Documents/de/train_de.csv")
    // TODO: assign timestamps.
    val ratingStream = data
      .flatMap(new RichFlatMapFunction[String, OUT] {

        override def flatMap(in: String, collector: Collector[OUT]): Unit = {
          val tuple = in.split(",")
          val result = (formatter.parseDateTime(tuple(0)), tuple(1).toInt, tuple(2).toInt, tuple(3).toInt)
          collector.collect(result)
        }
      })
      .keyBy(1, 2)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new GradientDescentFunction())

    // execute program
    streamingEnv.execute("Flink Streaming Scala API Skeleton")
  }

  class GradientDescentFunction extends RichWindowFunction[OUT, OUT, Tuple, TimeWindow] {
    private var w: MapState[Int, DenseVector] = _
    private var h: MapState[Int, DenseVector] = _

    // Number of users.
    val n = 100000

    // Number of items.
    val m = 100000

    def initFactorMatrix(map: lang.Iterable[Map.Entry[Int, DenseVector]],
                         size: Int): lang.Iterable[Map.Entry[Int, DenseVector]] = {
      if (map != null) {
        map
      } else {
        val scalaMap = collection.mutable.Map[Int, DenseVector]()
        for (i <- 0 until size) yield (scalaMap.put(i, DenseVector.zeros(Rank)))
        val javaMap = scalaMap.map { case (k, v) => k -> v }.asJava
        val newMap: MapState[Int, DenseVector] = null
        newMap.putAll(javaMap)
        newMap.entries()
      }
    }

    override def apply(key: Tuple, window: TimeWindow, input: Iterable[OUT], out: Collector[OUT]): Unit = {
      val tmpCurrentW = w.entries()
      val tmpCurrentH = h.entries()
      val currentW = initFactorMatrix(tmpCurrentW, n)
      val currentH = initFactorMatrix(tmpCurrentH, m)
    }
  }
}
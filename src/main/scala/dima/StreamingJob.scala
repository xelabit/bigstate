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

import org.apache.flink.api.common.io.FileInputFormat
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
  type OUT = (String, Int, Int, Int)
  val Rank = 100

  def main(args: Array[String]) {
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
    /*
     * TODO: assign timestamps.
     */
    val ratingStream = streamingEnv.readFile(getFileFormat("~/Documents/de/train_de.csv"), "~/Documents/de/train_de.csv")
      .mapWith {
        case (date, uid, iid, rating) => (formatter.parseDateTime(date), uid, iid, rating)
      }
      .keyBy(1, 2)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new GradientDescentFunction())

    // Set a big window on a stream in order to obtain max user/item ids (under assumption that they will show up during
    // this window.
//    val initialRead = ratingStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
//    val maxUid = initialRead.maxBy(1).mapWith { case (_, uid, _, _) => uid } /* I would like to get a single value from a stream*/

    val maxId = getMaxId(env)

    // User factor matrix.
    var w: DenseMatrix = DenseMatrix.zeros(maxId._1, Rank)

    // Item factor matrix.
    var h: DenseMatrix = DenseMatrix.zeros(Rank, maxId._2)

    // execute program
    streamingEnv.execute("Flink Streaming Scala API Skeleton")
  }

  def getFileFormat(path: String): FileInputFormat[OUT] = {
    val typeInfo: TypeInformation[OUT] = createTypeInformation[OUT]
    val tupleInfo = new TupleTypeInfo[OUT](typeInfo.getTypeClass, tupleInfo)
    val fileFormat = new TupleCsvInputFormat[OUT](new Path(path), tupleInfo)
    fileFormat
  }

  def getMaxId(env: ExecutionEnvironment): (Int, Int) = {
    val ratingData = env.readCsvFile("~/Documents/de/train_de.csv")
      .types(classOf[String], classOf[Int], classOf[Int], classOf[Int])
    val maxUid = ratingData.max(1).collect().get(0).f1
    val maxIid = ratingData.max(2).collect().get(0).f2
    (maxUid, maxIid)
  }

  class GradientDescentFunction extends RichWindowFunction[OUT, OUT, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Int, Int, Int)],
                       out: Collector[(String, Int, Int, Int)]): Unit = {
      
    }
  }
}
//  KeyedStream[(DateTime, Int, Int, Int), Tuple]
//  WindowedStream[(DateTime, Int, Int, Int), Tuple, TimeWindow]
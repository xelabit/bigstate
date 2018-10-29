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

package dima.misc

import org.apache.flink.api.scala._
import org.joda.time.format.DateTimeFormat

/**
 * Skeleton for a Flink Batch Job.
 *
 * For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your appliation into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
object BatchJob {

  def main(args: Array[String]) {
    def getHash(x: Int, partition: Int, flag: Boolean, partitionSize: Int, leftover: Int): Int = {
      var f = flag
      var p = partition
      var ps = partitionSize
      var l = leftover
      if (f) {
        ps += 1
        l -= 1
      }
      if (l > 0) f = true
      if (x <= ps) p
      else {
//        ps -= 1
        ps += ps
        p += 1
        getHash(x, p, f, ps, l)
      }
    }

    /**
      * Get the number of a block (substratum), to which a record should be assigned.
      * @param id user/item id of a point.
      * @param maxId largest user/item id. We assume we know this value in advance. However, this is not usually the
      *              case in endless stream tasks.
      * @param n a size of parallelism.
      * @return an id of a substratum along user or item axis.
      */
    def partitionId(id: Int, maxId: Int, n: Int): Int = {
      val partitionSize = maxId / n //286
      val leftover = maxId - partitionSize * n //1
      val flag = leftover == 0 //false
      if (id <= maxId) getHash(id, 0, flag, partitionSize, leftover)
      else -1
    }
    print(partitionId(572, 573, 2))
  }
}

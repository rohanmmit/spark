/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

/**
 * Class that keeps tracks of which range of map output partitions are needed
 * @param index of this coalesced partition
 * @param startIndex of map output partition needed
 * @param endIndex of map output partitions requested
 */
private[spark] class ShuffledJoinPartition(val index: Int, val startIndex: Int, val endIndex: Int)
  extends Partition {

  override def hashCode(): Int = index
}

/**
 * :: DeveloperApi ::
 *  An rdd strategy that supports the join strategy by using the shuffle
 *  method.
 *
 * @param dependency1 a ShuffleDependency for the first rdd
 * @param dependency2 a ShuffleDependency to second rdd
 */
@DeveloperApi
class ShuffleJoinRDD[K, V, W](var dependency1: ShuffleDependency[K, V, V],
                             var dependency2: ShuffleDependency[K, W, W],
                             val numReducers: Int)
  extends RDD[(K, (V, W))](dependency1.rdd.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = List(dependency1, dependency2)

  override def getPartitions: Array[Partition] = {
    val numTotalPartitions = dependency1.partitioner.numPartitions
    val numPartitionPerReducer = numTotalPartitions / numReducers
    val remainder = numTotalPartitions % numReducers;
    Array.tabulate[Partition](numReducers) { i =>
      var extraPartition = 0
      var shift = remainder
      if (i < remainder) {
         extraPartition = 1
         shift = i
      }
      new ShuffledJoinPartition(i, numPartitionPerReducer * i + shift,
        shift + numPartitionPerReducer * (i + 1) + extraPartition)
    }
  }

  override def compute(p: Partition, context: TaskContext): Iterator[(K, (V,W))] = {
    val sp = p.asInstanceOf[ShuffledJoinPartition]
    val reader1 = SparkEnv.get.shuffleManager.getReader(
      dependency1.shuffleHandle, None, sp.startIndex, sp.endIndex, context)
      .read()
      .asInstanceOf[Iterator[(K, V)]]
    val reader2 = SparkEnv.get.shuffleManager.getReader(
      dependency2.shuffleHandle, None, sp.startIndex, sp.endIndex, context)
      .read()
      .asInstanceOf[Iterator[(K, W)]]
    val map = new HashMap[K, ArrayBuffer[V]]
    for (pair <- reader1) {
              map.getOrElseUpdate(pair._1, new ArrayBuffer()) += pair._2
    }
    reader2.flatMap { pair =>
              val k = pair._1
              val w = pair._2
              val values = map.getOrElse(k, Nil)
              values.map(v => (k, (v, w)))
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency1 = null
    dependency2 = null
  }
}

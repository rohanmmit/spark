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

import scala.collection.mutable.{ArrayBuffer, HashMap}

private[spark] class BroadcastJoinPartition(val index: Int, val broadcastMapTask: Int)
  extends Partition {
  override def hashCode(): Int = index
}

/**
 * An rdd that provides a join functionality using a broadcast join. This would be useful in cases
 * where one shuffle dependency is substantially bigger then the other ones. It creates one
 * reduce task for each of the map tasks of the bigger dependency.
 *
 * @param bigDependency is a shuffle dependency on the bigger rdd
 * @param smallDependency is a shuffle dependency on the smaller rdd
 */
@DeveloperApi
class BroadcastJoinRdd[K, V, W](var bigDependency: ShuffleDependency[K, V, V],
                                var smallDependency: ShuffleDependency[K, W, W])

  extends RDD[ (K, (V, W))](bigDependency.rdd.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = List(bigDependency, smallDependency)

  override def getPartitions: Array[Partition] = {
    val numPartitions =  bigDependency.rdd.partitions.length
    Array.tabulate[Partition](numPartitions) { i =>
      new BroadcastJoinPartition(i,i)
    }
  }

  override def compute(p: Partition, context: TaskContext): Iterator[(K, (V,W))] = {
    val partition = p.asInstanceOf[BroadcastJoinPartition]
    val broadcastMapTask = Some(partition.broadcastMapTask)
    val numBigMapOutputTasks = bigDependency.partitioner.numPartitions
    val numSmallOutputTasks = smallDependency.partitioner.numPartitions
    val bigReader = SparkEnv.get.shuffleManager.getReader(
      bigDependency.shuffleHandle, broadcastMapTask, 0, numBigMapOutputTasks, context)
      .read()
      .asInstanceOf[Iterator[(K, V)]]
    val smallReader = SparkEnv.get.shuffleManager.getReader(
      smallDependency.shuffleHandle, None, 0, numSmallOutputTasks, context)
      .read()
      .asInstanceOf[Iterator[(K, W)]]
    val map = new HashMap[K, ArrayBuffer[W]]
    for (pair <- smallReader) {
      map.getOrElseUpdate(pair._1, new ArrayBuffer()) += pair._2
    }
    bigReader.flatMap { pair =>
      val k = pair._1
      val v = pair._2
      val values = map.getOrElse(k, Nil)
      values.map(w => (k, (v, w)))
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val part = partition.asInstanceOf[BroadcastJoinPartition]
    val tracker = SparkEnv.get.mapOutputTracker
    tracker.getMapOutputLocation(bigDependency.shuffleId, part.broadcastMapTask).map(_.host).toList
  }

  override def clearDependencies() {
    super.clearDependencies()
    bigDependency = null
    smallDependency = null
  }
}

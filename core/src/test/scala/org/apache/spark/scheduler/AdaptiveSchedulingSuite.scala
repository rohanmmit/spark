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

package org.apache.spark.scheduler

import org.apache.spark._

object AdaptiveSchedulingSuiteState {
  var tasksRun = 0

  def clear(): Unit = {
    tasksRun = 0
  }
}

class AdaptiveSchedulingSuite extends SparkFunSuite with LocalSparkContext {
  test("simple use of submitMapStage") {
    try {
      sc = new SparkContext("local", "test")
      val rdd = sc.parallelize(1 to 3, 3).map { x =>
        AdaptiveSchedulingSuiteState.tasksRun += 1
        (x, x)
      }
      val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(2))
      val shuffled = new CustomShuffledRDD[Int, Int, Int](dep)
      sc.submitMapStage(dep).get()
      assert(AdaptiveSchedulingSuiteState.tasksRun == 3)
      assert(shuffled.collect().toSet == Set((1, 1), (2, 2), (3, 3)))
      assert(AdaptiveSchedulingSuiteState.tasksRun == 3)
    } finally {
      AdaptiveSchedulingSuiteState.clear()
    }
  }

  test("fetching multiple map output partitions per reduce") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(0 to 2, 3).map(x => (x, x))
    val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(3))
    val shuffled = new CustomShuffledRDD[Int, Int, Int](dep, Array(0, 2))
    assert(shuffled.partitions.length === 2)
    assert(shuffled.glom().map(_.toSet).collect().toSet == Set(Set((0, 0), (1, 1)), Set((2, 2))))
  }

  test("fetching all map output partitions in one reduce") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(0 to 2, 3).map(x => (x, x))
    // Also create lots of hash partitions so that some of them are empty
    val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(5))
    val shuffled = new CustomShuffledRDD[Int, Int, Int](dep, Array(0))
    assert(shuffled.partitions.length === 1)
    assert(shuffled.collect().toSet == Set((0, 0), (1, 1), (2, 2)))
  }

  test("more reduce tasks than map output partitions") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(0 to 2, 3).map(x => (x, x))
    val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(3))
    val shuffled = new CustomShuffledRDD[Int, Int, Int](dep, Array(0, 0, 0, 1, 1, 1, 2))
    assert(shuffled.partitions.length === 7)
    assert(shuffled.collect().toSet == Set((0, 0), (1, 1), (2, 2)))
  }

  test("check the shuffle join is working") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(1 to 10, 5).map(x => (x, x))
    val dep1 = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(10))
    val rdd2 = sc.parallelize(1 to 10, 5).map(x => (x, 2*x))
    val dep2 = new ShuffleDependency[Int, Int, Int](rdd2, new HashPartitioner(10))
    val rdd3 = new org.apache.spark.rdd.ShuffleJoinRDD(dep2, dep1, 3)

    val result =Array((1,(2,1)), (2,(4,2)), (3,(6,3)), (10,(20,10)), (4,(8,4)), (5,(10,5)), (6,(12,6)), (7,(14,7)), (8,(16,8)), (9,(18,9)))
    assert(rdd3.collect.deep == result.deep)
    //test overlap fits
    val rdd4 = sc.parallelize(5 to 20, 5).map(x => (x, x))
    val dep3 = new ShuffleDependency[Int, Int, Int](rdd4, new HashPartitioner(10))
    val rdd5 = new org.apache.spark.rdd.ShuffleJoinRDD(dep2, dep3, 3)
    val result2 = Array((10,(20,10)), (5,(10,5)), (6,(12,6)), (7,(14,7)), (8,(16,8)), (9,(18,9)))
    assert(rdd5.collect.deep == result2.deep)
    // test multiple values of joining
    val dep4 = new ShuffleDependency[Int, Product2[Int,Int], Product2[Int,Int]](rdd3, new HashPartitioner(3))
    val dep5 = new ShuffleDependency[Int, Product2[Int,Int], Product2[Int,Int]](rdd3, new HashPartitioner(3))
    val rdd6 = new org.apache.spark.rdd.ShuffleJoinRDD(dep4, dep5, 3)
    val result3 = Array((3,((6,3),(6,3))), (6,((12,6),(12,6))), (9,((18,9),(18,9))), (1,((2,1),(2,1))), (10,((20,10),(20,10))), (4,((8,4),(8,4))), (7,((14,7),(14,7))), (2,((4,2),(4,2))), (5,((10,5),(10,5))), (8,((16,8),(16,8))))
    assert(rdd6.collect.deep == result3.deep)
  }

  test("check the broadcast join is working") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(1 to 10, 5).map(x => (x, x))
    val dep1 = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(3))
    val rdd2 = sc.parallelize(1 to 10, 5).map(x => (x, 2*x))
    val dep2 = new ShuffleDependency[Int, Int, Int](rdd2, new HashPartitioner(3))
    val rdd3 = new org.apache.spark.rdd.BroadcastJoinRdd(dep2, dep1)
    assert(rdd3.partitions.length == 5)
    rdd3.collect
    val result = Array((1,(2,1)), (2,(4,2)), (3,(6,3)), (4,(8,4)), (6,(12,6)), (5,(10,5)), (7,(14,7)), (8,(16,8)), (9,(18,9)), (10,(20,10)));
    assert(rdd3.collect.deep == result.deep)
    //test overlap fits
    val rdd4 = sc.parallelize(5 to 20, 5).map(x => (x, x))
    val dep3 = new ShuffleDependency[Int, Int, Int](rdd4, new HashPartitioner(3))
    val rdd5 = new org.apache.spark.rdd.BroadcastJoinRdd(dep2, dep3)
    val result2 = Array((6,(12,6)), (5,(10,5)), (7,(14,7)), (8,(16,8)), (9,(18,9)), (10,(20,10)))
    assert(rdd5.partitions.length == 5)
    assert(rdd5.collect.deep == result2.deep)
    // test multiple values of joining
    val dep4 = new ShuffleDependency[Int, Product2[Int,Int], Product2[Int,Int]](rdd3, new HashPartitioner(3))
    val dep5 = new ShuffleDependency[Int, Product2[Int,Int], Product2[Int,Int]](rdd3, new HashPartitioner(3))
    val rdd6 = new org.apache.spark.rdd.BroadcastJoinRdd(dep4, dep5)
    val result3 = Array((1,((2,1),(2,1))), (2,((4,2),(4,2))), (3,((6,3),(6,3))), (4,((8,4),(8,4))), (6,((12,6),(12,6))), (5,((10,5),(10,5))), (7,((14,7),(14,7))), (8,((16,8),(16,8))), (9,((18,9),(18,9))), (10,((20,10),(20,10))))
    assert(rdd6.collect.deep == result3.deep)
  }

}

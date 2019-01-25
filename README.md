# Spark Scala Top N Nonunique
[![Made with Scala](https://img.shields.io/badge/Made%20with-Scala-yellow.svg)](https://img.shields.io/badge/Made%20with-Scala-yellow.svg) [![Powered by Spark](https://img.shields.io/badge/Powered%20by-Spark-red.svg)](https://img.shields.io/badge/Powered%20by-Spark-red.svg) [![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) 

This project implements the top N design pattern assuming that input keys are not unique. Rather than simply using Spark's sorting functions, such as `top()` or `takeOrdered()`, in this project I provide a low-level solution.

## Up & Running

In IntelliJ, execute `sbt package`.

Run `spark-submit`.
```sh
~/workspace/scala/spark-scala-top-n-nonunique$ spark-submit --master local --class yungshun.chang.topnnonunique.TopNNonunique target/scala-2.11/spark-scala-top-n-nonunique_2.11-0.1.jar datasets/input/
76 	 F
118 	 B
```

## Top N

Given a set of (key-as-string, value-as-integer) pairs, say we want to create a top N (where `N > 0`) list. Finding a top N list is categorized as a filtering pattern (i.e., you filter out data and find the top 10 list).

The main point is that before finding the top N of any set of `(K, V)` pairs, we have to make sure that all `K`s are unique. The main steps of the algorithm is:
1. Make all `K`s unique. To make `K`s unique, we will map our input into `JavaPairRDD<K, V>` pairs and then `reduceByKey()`.
2. Partition all unique `(K, V)` pairs into M partitions.
3. Find the top N for each partition (we'll call this a local top N).
4. Find the top N from all local top Ns.

Input format is `<String>,<Int>`. Perform `map` and `reduceByKey` to create unique keys.
```scala
val input = sc.textFile(path)

val kv = input.map(line => {
  val tokens = line.split(",")
  (tokens(0), tokens(1).toInt)
})

val uniqueKeys = kv.reduceByKey(_ + _)
```

Note that because by default `sortedMap` is sorted by key, we perform `tuple.swap` first to make it ordering by original value. And then Scala's `takeRight` method get last N elements for us. This won't throw exception when `SortedMap`'s length is not enough.
```scala
import Ordering.Implicits._

val partitions = uniqueKeys.mapPartitions(itr => {
  var sortedMap = SortedMap.empty[Int, String]
  itr.foreach { 
    tuple => {
      sortedMap += tuple.swap
      if (sortedMap.size > N.value) {
        sortedMap = sortedMap.takeRight(N.value)
      }
    }
  }
  sortedMap.takeRight(N.value).toIterator
})
```
So that we get the local top N.

> What's the difference between an RDD's `map` and `mapPartitions` method? The method `map` converts each
> element of the source RDD into a single element of the result RDD by applying a function. `mapPartitions`
> converts each partition of the source RDD into multiple elements of the result (possibly none).
> And does `flatMap` behave like `map` or like `mapPartitions`? Neither, `flatMap` works on a single element
> (as `map`) and produces multiple elements of the result (as `mapPartitions`).

And then `collect` all local top N to find our final top N.
```scala
val alltop10 = partitions.collect()
val finaltop10 = SortedMap.empty[Int, String].++:(alltop10)
val resultUsingMapPartition = finaltop10.takeRight(N.value)

resultUsingMapPartition.foreach {
  case (k, v) => println(s"$k \t ${v.mkString(",")}")
}
```
Because we already performed `tuple.swap`, the result is in `<Int> <String>` format. And we set `val N = sc.broadcast(2)` in the beginning, we get the top 2 list like the example finally.

More concise code.
```scala
val createCombiner = (v: Int) => v
val mergeValue = (a: Int, b: Int) => (a + b)
val moreConciseApproach = kv.combineByKey(createCombiner, mergeValue, mergeValue).map(_.swap)
  .groupByKey().sortByKey(false).take(N.value)

moreConciseApproach.foreach {
  case (k, v) => println(s"$k \t ${v.mkString(",")}")
}
```

## Spark SQL

It's not recommended processing your data in this low-level approach any more. We had better move to Spark SQL.

After initializing `SparkSession` as `spark`, we can simply do the following in `spark-shell` to achieve the same goal:
```scala
val df = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load("/home/yungshun/workspace/scala/spark-scala-top-n-nonunique/datasets/input/*.csv").groupBy(col("_c0")).agg(sum(col("_c1")) as "_c1").sort(desc("_c1")).limit(2).show()
+---+-----+                                                       
|_c0|  _c1|
+---+-----+
|  B|118.0|
|  F| 76.0|
+---+-----+

df: Unit = ()
```

## Tech

This project uses:

* [IntelliJ](https://www.jetbrains.com/idea/) - a Java integrated development environment (IDE) for developing computer software developed by JetBrains.
* [Spark](https://spark.apache.org/) - a unified analytics engine for large-scale data processing.

## Todos

 - Use Spark SQL.
 - This article, [The Differences between Partition Key, Composite Key, and Clustering columns in Cassandra](https://www.bmc.com/blogs/cassandra-clustering-columns-partition-composite-key/), is a good resource to read.

## License
[Spark Scala Top N Nonunique](https://github.com/yungshun317/spark-scala-top-n-nonunique) is released under the [MIT License](https://opensource.org/licenses/MIT) by [yungshun317](https://github.com/yungshun317).

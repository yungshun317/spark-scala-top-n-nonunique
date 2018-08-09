package yungshun.chang.topnnonunique

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap

object TopNNonunique {
  def main(args: Array[String]): Unit = {
    if (args.size < 1) {
      println("Usage: TopNNonunique <input>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("TopNNonunique")
    val sc = new SparkContext(sparkConf)

    val N = sc.broadcast(2)
    val path = args(0)

    val input = sc.textFile(path)
    val kv = input.map(line => {
      val tokens = line.split(",")
      (tokens(0), tokens(1).toInt)
    })

    val uniqueKeys = kv.reduceByKey(_ + _)
    import Ordering.Implicits._
    val partitions = uniqueKeys.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, String]
      itr.foreach { tuple =>
      {
        sortedMap += tuple.swap
        if (sortedMap.size > N.value) {
          sortedMap = sortedMap.takeRight(N.value)
        }
      }
      }
      sortedMap.takeRight(N.value).toIterator
    })

    val alltop10 = partitions.collect()
  }
}

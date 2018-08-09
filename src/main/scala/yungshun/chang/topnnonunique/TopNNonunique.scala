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
  }
}

package fastcode.s19.team027

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.immutable

object GitHubRecommendation {
  val MIN_USER_RATING = 10
  val MAX_REL_REPO = 100

  def computeScore(event: String): Long = event match {
    case "WatchEvent" => 10
    case "ForkEvent" => 6
    case "IssuesEvent" => 1
    case "PullRequestEvent" => 2
  }

  def mergeMap(m1: mutable.HashMap[String, Long], m2: mutable.HashMap[String, Long]) = {
    for ((k, v) <- m2) {
      val newV = m1.getOrElse(k, 0L) + v
      m1.put(k, newV)
    }
    m1
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("GitHubRecommendation")
      .getOrCreate()
    val sc = spark.sparkContext

    val fileName = if (args.length > 0) args(0) else ""

    val inputData = spark.read.format("json").load(s"s3a://ph.fastcode.s19.github-data/$fileName")

    val userRepoScore = inputData.rdd
      .map(r => ((r.getAs[String]("user"), r.getAs[String]("repo")), computeScore(r.getAs[String]("type"))))
      .reduceByKey(_ + _)
      .filter { case (_, score) => score >= MIN_USER_RATING }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val partitioner = new HashPartitioner(1000)

    val userData = userRepoScore.map { case ((user, repo), score) => (user, (repo, score)) }
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val repoData = userRepoScore.map { case ((user, repo), score) => (repo, (user, score)) }
      .partitionBy(partitioner)

    val similarity = userData.join(userData.groupByKey())
      .map { case (_, ((repo1, score1), iter)) =>
        (repo1, mutable.HashMap(iter.map({ case (repo2, score2) => repo2 -> score1 * score2 }).toSeq: _*))
      }
      .reduceByKey(mergeMap)
      .mapValues { scoreMap => scoreMap.view.toList.sortBy(_._2)(Ordering[Long].reverse).take(MAX_REL_REPO) }

//    val similarity = userData.join(userData)
//      .map { case (_, ((repo1, score1), (repo2, score2))) => (repo1, mutable.HashMap(repo2 -> score1 * score2)) }
//      .reduceByKey(mergeMap)
//      .flatMapValues { scoreMap => scoreMap.view.toList.sortBy(_._2)(Ordering[Long].reverse).take(MAX_REL_REPO) }

    /* result ((user, repo) => recommendationScore) */
    val result = repoData.join(similarity)
      .map { case (_, ((user, score1), iter2)) =>
        (user, mutable.HashMap((for ((repo, score2) <- iter2) yield repo -> score1 * score2): _*))
      }
      .reduceByKey(mergeMap)
      .mapValues { scoreMap => scoreMap.view.toList.sortBy(_._2)(Ordering[Long].reverse).take(MAX_REL_REPO) }
      .coalesce(100)
    // userResult.take(10).foreach(println)
    result.saveAsTextFile(s"s3a://ph.fastcode.s19.gh-output/result-${System.currentTimeMillis()}")
  }
}

package fastcode.s19.team027

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.immutable

object GitHubRecommendation {
  def computeScore(event: String): Long = event match {
    case "WatchEvent" => 10
    case "ForkEvent" => 6
    case "IssuesEvent" => 1
    case "PullRequestEvent" => 2
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
      .filter { case (_, score) => score >= 10 }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val partitioner = new HashPartitioner(1000)

    val userData = userRepoScore.map { case ((user, repo), score) => (user, (repo, score)) }
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val repoData = userRepoScore.map { case ((user, repo), score) => (repo, (user, score)) }
      .partitionBy(partitioner)

    /* similarity (repo1 => (repo2, similarityScore)) */
//    val similarity = userData.join(userData)
//      .map { case (_, ((repo1, score1), (repo2, score2))) => ((repo1, repo2), score1 * score2) }
//      .reduceByKey(_ + _)
//      .map { case ((repo1, repo2), score) => (repo1, (repo2, score)) }
//      .partitionBy(partitioner)

//    val similarity = userData.join(userData)
//      .map { case (_, ((repo1, score1), (repo2, score2))) => (repo1, mutable.HashMap(repo2 -> score1 * score2)) }
//      .reduceByKey((m1, m2) => {
//        for ((k, v) <- m2) {
//          val newV = m1.getOrElse(k, 0) + v
//          m1.put(k, newV)
//        }
//        m1
//      })
//      .flatMapValues { scoreMap => scoreMap.view }

    val similarity = repoData.join(userData)
      .map { case (_, ((repo1, score1), (repo2, score2))) => (repo1, mutable.HashMap(repo2 -> score1 * score2)) }
      .reduceByKey((m1, m2) => {
        for ((k, v) <- m2) {
          val newV = m1.getOrElse(k, 0) + v
          m1.put(k, newV)
        }
        m1
      })
      .flatMapValues { scoreMap => scoreMap.view }

    /* result ((user, repo) => recommendationScore) */
    val result = repoData.join(similarity)
      .map { case (_, ((user, score1), (repo, score2))) => ((user, repo), score1 * score2) }
      .reduceByKey(_+_)
    val userResult = result.map(x => (x._1._1, (x._1._2, x._2))).groupByKey()
    // userResult.take(10).foreach(println)
    userResult.mapValues(_.toArray.sortWith(_._2 > _._2).mkString(" ")).saveAsTextFile(s"s3a://ph.fastcode.s19.gh-output/result-${System.currentTimeMillis()}")
  }
}

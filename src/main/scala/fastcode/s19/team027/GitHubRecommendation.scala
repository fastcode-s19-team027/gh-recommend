package fastcode.s19.team027

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object GitHubRecommendation {
  def computeScore(event: String): Int = event match {
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

    val fileName = if (args.length > 0) args(0) else ""

    val inputData = spark.read.format("json").load(s"s3a://ph.fastcode.s19.github-data/$fileName")

    val userRepoScore = inputData.rdd
      .map(r => ((r.getAs[String]("user"), r.getAs[String]("repo")), computeScore(r.getAs[String]("type"))))
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val partitioner = new HashPartitioner(1000)

    val userData = userRepoScore.map { case ((user, repo), score) => (user, (repo, score)) }
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    /* similarity (repo1 => (repo2, similarityScore)) */
    val similarity = userData.join(userData)
      .map { case (_, ((repo1, score1), (repo2, score2))) => ((repo1, repo2), score1 * score2) }
      .reduceByKey(_ + _)
      .map { case ((repo1, repo2), score) => (repo1, (repo2, score)) }
      .partitionBy(partitioner)

    /* result ((user, repo) => recommendationScore) */
    val repoData = userRepoScore.map { case ((user, repo), score) => (repo, (user, score)) }
      .partitionBy(partitioner)
    val result = repoData.join(similarity)
      .map { case (_, ((user, score1), (repo, score2))) => ((user, repo), score1 * score2) }
      .reduceByKey(_+_)
    val userResult = result.map(x => (x._1._1, (x._1._2, x._2))).groupByKey()
    // userResult.take(10).foreach(println)
    userResult.mapValues(_.toArray.sortWith(_._2 > _._2).mkString(" ")).saveAsTextFile(s"s3a://ph.fastcode.s19.gh-output/result-${System.currentTimeMillis()}")
  }
}

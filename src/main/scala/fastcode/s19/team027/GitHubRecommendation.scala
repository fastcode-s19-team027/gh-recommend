package fastcode.s19.team027

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

    val rddData = inputData.rdd
      .map(r => ((r.getAs[String]("user"), r.getAs[String]("repo")), computeScore(r.getAs[String]("type"))))
      .reduceByKey(_+_)
      .persist(StorageLevel.DISK_ONLY)

    val userData = rddData.map { case ((user, repo), score) => (user, (repo, score)) } groupByKey

    /* similarity (repo1 => (repo2, similarityScore)) */
    val simData = userData.flatMap { case (_, repoScore) =>
      repoScore.flatMap {
    		repo1 => repoScore.map(repo2 => ((repo1._1, repo2._1), repo1._2 * repo2._2))
    	}
    }.reduceByKey(_+_).filter { case (_, score) => score > 0 }
    val similarity = simData.map(x => (x._1._1, (x._1._2, x._2))).groupByKey()

    val repoData = rddData.map(x => (x._1._2, (x._1._1, x._2)))

    /* result ((user, repo) => recommendationScore) */
    val recData = repoData.join(similarity)
    val rec = recData.flatMap {
    	case (repo, (score, sim)) => {
    		sim.map(x => ((score._1, x._1), x._2 * score._2))
    	}
    }
    val result = rec.reduceByKey(_+_)
    val userResult = result.map(x => (x._1._1, (x._1._2,x._2))).groupByKey()
    // userResult.take(10).foreach(println)
    userResult.map(x => (x._1,x._2.toArray.sortWith(_._2 > _._2).mkString(" "))).saveAsTextFile("s3a://ph.fastcode.s19.gh-output/result")
  }
}

package fastcode.s19.team027

import org.apache.spark.sql.SparkSession

object GitHubRecommendation {
  def computeScore(event: String): Int = {
  	var score = event match {
  		case "WatchEvent" => 10
  		case "ForkEvent" => 6
  		case "IssuesEvent" => 1
  		case "PullRequestEvent" => 2
  	}
  	return score
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("GitHubRecommendation")
      .getOrCreate()
    val sc = spark.sparkContext

    val fileName = if (args.length > 0) args(0) else ""

    val inputData = spark.read.format("json").load(s"s3a://ph.fastcode.s19.github-data/$fileName")
    // var inputData = spark.read.format("json").load(s"github-data/$fileName")
    // inputData.take(10).foreach(println)
    println("Converting data...")
    var rddData = inputData.rdd.map(r => ((r.getAs[String]("user"), r.getAs[String]("repo")), computeScore(r.getAs[String]("type"))))
    
    /* rddData ((user, repo) => score) */
    rddData = rddData.reduceByKey(_+_)

    /* user (user => [(repo, score)]) */
    var userData = rddData.map(x => (x._1._1, (x._1._2, x._2)))
    var user = userData.groupByKey()
    println("Computing Similarity...")
    /* similarity (repo1 => (repo2, similarityScore)) */
    val simData = user.flatMap {
    	x => val temp = x._2
    	temp.flatMap {
    		repo1 => temp.map(repo2 => ((repo1._1, repo2._1), repo1._2 * repo2._2))
    	}
    }.reduceByKey(_+_)
    var similarity = simData.map(x => (x._1._1, (x._1._2, x._2))).groupByKey()
    var repoData = rddData.map(x => (x._1._2, (x._1._1, x._2)))
    println("Computing Recommendation Score")
    /* result ((user, repo) => recommendationScore) */
    var recData = repoData.join(similarity)
    var rec = recData.flatMap {
    	case (repo, (score, sim)) => {
    		sim.map(x => ((score._1, x._1), x._2 * score._2))
    	}
    }
    var result = rec.reduceByKey(_+_)
    var userResult = result.map(x => (x._1._1, (x._1._2,x._2))).groupByKey()
    // userResult.take(10).foreach(println)
    userResult.map(x => (x._1,x._2.toArray.sortWith(_._2 > _._2).mkString(" "))).saveAsTextFile("s3a://ph.fastcode.s19.gh-output/result")
  }
}

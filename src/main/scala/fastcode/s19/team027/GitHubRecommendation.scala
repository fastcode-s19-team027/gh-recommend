package fastcode.s19.team027

import org.apache.spark.sql.SparkSession

object GitHubRecommendation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("GitHubRecommendation")
      .getOrCreate()
    val sc = spark.sparkContext

    val fileName = if (args.length > 0) args(0) else ""

    val inputData = spark.read.format("json").load(s"s3a://ph.fastcode.s19.github-data/$fileName")

    inputData.take(10).foreach(println)
  }
}

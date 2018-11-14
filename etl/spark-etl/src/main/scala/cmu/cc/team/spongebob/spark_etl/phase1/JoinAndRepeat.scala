package cmu.cc.team.spongebob.spark_etl.phase1

import org.apache.spark.sql.SparkSession

object JoinAndRepeat {
  val s3_access_key = sys.env("S3_ACCESS_KEY")
  val s3_secret_key = sys.env("S3_SECRET_KEY")

  val spark = new SparkSession()
  import spark.implicits._
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3_access_key)
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3_secret_key)

  def main(args: Array[String]): Unit = {
    val contactTweetInputFilepath = args(0)
    val contactUserInputFilepath = args(1)
    val outputFilepath = args(2)

    val tweetsDF = spark.read.parquet(contactTweetInputFilepath)
    val usersDF = spark.read.parquet(contactUserInputFilepath)

    val tweetRepeat = tweetsDF
      .union(tweetsDF.select($"user2_id".as("user1_id"),
        $"user1_id".as("user2_id"), $"tweet_text", $"created_at", $"intimacy_score"))

    val joinedDF = tweetRepeat.join(usersDF, usersDF.col("id") === tweetRepeat.col("user1_id"), "left")
      .drop("id")
      .withColumnRenamed("description", "user1_desc")
      .withColumnRenamed("screen_name", "user1_screen_name")
      .join(usersDF,  usersDF.col("id") === tweetRepeat.col("user2_id"), "left")
      .drop("id")
      .withColumnRenamed("description", "user2_desc")
      .withColumnRenamed("screen_name", "user2_screen_name")

    // load to mysql
    joinedDF.write.json(outputFilepath)
  }
}

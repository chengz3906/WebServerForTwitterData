/**
  * ETL for query 2, user intimacy ranking.
  * Saves a contact_tweet dataframe containing all replies and retweets, and a user dataframe
  * containing user screen_name and description.
  */

package cmu.cc.team.spongebob.spark_etl

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, to_timestamp, when}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object UserIntimacy {
  // create Spark session
  private val sparkConf = new SparkConf()
  @transient private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  import spark.implicits._
  @transient private val sc = spark.sparkContext

  case class ContactTweet(user1_id: Long, user2_id: Long,
                          tweet_text: String, created_at: Long)

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val inputFilename = args(1)
    val outputPath = args(2)
    val outputFilenameSuffix = if (args.length == 4) s"_${args(3)}" else ""

    val contactTweetsDF: DataFrame = loadAndFilterTweets(inputPath, inputFilename)

    val usersDF: DataFrame = extractUsersDF(contactTweetsDF)
    val (repliesDF: DataFrame, retweetsDF: DataFrame, contactTweetTextsDF: Dataset[Row]) =
      extractContactTweets(contactTweetsDF)
    val userIntimacyDF: DataFrame = computeIntimacyScore(repliesDF, retweetsDF)

    val contactTweetsIntimacyDF = contactTweetTextsDF
      .join(userIntimacyDF, List("user1_id", "user2_id"))
      .dropDuplicates("user1_id", "user2_id", "tweet_text")
      .cache()

    // write outputs
    usersDF.write.
      parquet(s"$outputPath/contact_user_parquet$outputFilenameSuffix")
    contactTweetsIntimacyDF.write.
      parquet(s"$outputPath/contact_tweet_parquet$outputFilenameSuffix")
  }

  /**
    * Compute intimacy scores between all users.
    */
  private def computeIntimacyScore(repliesDF: DataFrame, retweetsDF: DataFrame) = {
    val retweetCounts = retweetsDF.as[ContactTweet].rdd
      .map { case ContactTweet(user1ID, user2ID, text, createdAt) =>
        ((math.min(user1ID, user2ID), math.max(user1ID, user2ID)), 1)
      } // retweet counts as 1
      .reduceByKey(_ + _)

    val replyCounts = repliesDF.as[ContactTweet].rdd
      .map { case ContactTweet(user1ID, user2ID, text, createdAt) =>
        ((math.min(user1ID, user2ID), math.max(user1ID, user2ID)), 2)
      } // reply counts as 2
      .reduceByKey(_ + _)

    val intimacyRDD = replyCounts.union(retweetCounts)
      .reduceByKey(_ + _)
      .mapValues(c => 1 + math.log(1 + c))
      .flatMap { case ((userID1, userID2), intimacyScore)
      => List((userID1, userID2, intimacyScore), (userID2, userID1, intimacyScore))
      } // repeat

    intimacyRDD.toDF("user1_id", "user2_id", "intimacy_score")
  }

  /**
    * Extracts contact tweets.
    */
  private def extractContactTweets(contactTweetsDF: DataFrame) = {
    val repliesDF = contactTweetsDF
      .filter("in_reply_to_user_id is not null")
      .select($"user_id".as("user1_id"), $"reply_user_id".as("user2_id"),
        $"text".as("tweet_text"), $"created_at")

    val retweetsDF = contactTweetsDF
      .filter("in_reply_to_user_id is null")
      .select($"user_id".as("user1_id"), $"retweet_user_id".as("user2_id"),
        $"text".as("tweet_text"), $"created_at")

    val contactTweetTextsDF = repliesDF.union(retweetsDF)

    (repliesDF, retweetsDF, contactTweetTextsDF)
  }

  /**
    * Extract contact user information.
    */
  private def extractUsersDF(contactTweetsDF: DataFrame) = {
    val usersDF = contactTweetsDF
      .select($"user_id".as("id"), $"user_description".as("description"),
        $"user_name".as("screen_name"), $"user_created_at".as("created_at"))
      .union(contactTweetsDF.select($"retweet_user_id".as("id"),
        $"retweet_user_desc".as("description"),
        $"retweet_user_screen_name".as("screen_name"),
        $"retweet_user_created_at".as("created_at")))

    // pick the most recent user_name and description
    val w = Window.partitionBy($"id").orderBy($"created_at".desc)
    val usersConsolidated = usersDF
      .filter("(screen_name is not null and screen_name != '') or (description is not null and description != '')")
      .withColumn("rn", row_number.over(w)).where($"rn" === 1)
      .drop("rn")
      .select("id", "description", "screen_name")
      .cache()
    usersConsolidated
  }

  /**
    * Load and filter tweets.
    */
  private def loadAndFilterTweets(inputPath: String, inputFilename: String) = {
    // read and filter tweet
    val tweetsDF = spark.read
      .option("mode", "DROPMALFORMED")
      .json(s"$inputPath/$inputFilename")

    tweetsDF.filter("id is not null").
      // keep only retweets and replies
      filter("in_reply_to_user_id is not null or retweeted_status.id is not null").
      // if it is retweet, retweet.user.id or retweet.user.id_str cannot be empty
      filter("in_reply_to_user_id is not null or (retweeted_status.user.id is not null or retweeted_status.user.id_str is not null)")
      .withColumn("retweeted_status.user.id",
      when($"in_reply_to_user_id".isNull.and($"retweeted_status.user.id".isNull),
        $"retweeted_status.user.id_str".cast("long"))
        .otherwise($"retweeted_status.user.id")) // convert retweet user_idstr to id
      .filter("user.id is not null or user.id_str is not null")
      .withColumn("user.id", when($"user.id".isNull,
        $"user.id_str".cast("long")).otherwise($"user.id")) // convert id_str to id
      .filter("entities.hashtags is not null and size(entities.hashtags) != 0")
      .filter("text is not null and text != ''")
      .filter("created_at is not null and created_at != ''")
      .filter($"lang".isNotNull.and($"lang".isin("ar", "en", "fr", "in", "pt", "es", "tr", "ja")))
      .filter("not (in_reply_to_user_id is not null and in_reply_to_user_id = user.id)") // filter reply to oneself
      .filter("not (in_reply_to_user_id is null and retweeted_status.user.id = user.id)") // filter retweet to oneself
      .dropDuplicates("id") // drop duplicated tweets
      .select($"id".as("tweet_id"), $"created_at", $"text",
      $"user.id".as("user_id"), $"user.screen_name".as("user_name"),
      $"user.description".as("user_description"), $"user.created_at".as("user_created_at"),
      $"retweeted_status.id".as("retweet_id"), $"retweeted_status.user.id".as("retweet_user_id"),
      $"retweeted_status.user.screen_name".as("retweet_user_screen_name"),
      $"retweeted_status.user.description".as("retweet_user_desc"),
      $"retweeted_status.user.created_at".as("retweet_user_created_at"),
      $"in_reply_to_user_id".as("reply_user_id"))
      .withColumn("created_at",
        to_timestamp($"created_at", "EEE MMM dd HH:mm:ss Z yyyy").cast("long")) // parse timestamp
      .withColumn("user_created_at", $"created_at")
      .withColumn("retweet_user_created_at", $"created_at")
      .cache()
  }
}

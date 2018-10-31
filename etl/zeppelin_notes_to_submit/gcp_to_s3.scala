/* this is a Zeppelin notebook
 * this notebook consumes original data and produces
 * a contact_user and contact_tweet datarame and store it
 * on S3
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import spark.implicits._

import org.apache.spark.sql.functions.{row_number, max, broadcast}
import org.apache.spark.sql.expressions.Window

sc.hadoopConfiguration.set("fs.s3a.access.key", s3_access_key)
sc.hadoopConfiguration.set("fs.s3a.secret.key",  s3_secret_key)

spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3_access_key)
spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3_secret_key)

val tweetsDF = spark.read
  .option("mode", "DROPMALFORMED")
  .option("badRecordsPath", "gs://twitter-etl/bad_records/")
  .json("gs://cmuccpublicdatasets/twitter/f18//part-r-00***.gz")

val contactTweetsDF = tweetsDF
  .filter("id is not null")
  .filter("in_reply_to_user_id is not null or retweeted_status.id is not null")   // keep only retweets and replies
  .filter("in_reply_to_user_id is not null or (retweeted_status.user.id is not null or retweeted_status.user.id_str is not null)")  // if it is retweet, retweet.user.id or retweet.user.id_str cannot be empty
  .withColumn("retweeted_status.user.id",
  when($"in_reply_to_user_id".isNull.and($"retweeted_status.user.id".isNull),
    $"retweeted_status.user.id_str".cast("long"))
    .otherwise($"retweeted_status.user.id"))   // convert retweet user_idstr to id
  .filter("user.id is not null or user.id_str is not null")
  .withColumn("user.id", when($"user.id".isNull, $"user.id_str".cast("long")).otherwise($"user.id")) // convert id_str to id
  .filter("entities.hashtags is not null and size(entities.hashtags) != 0")
  .filter("text is not null and text != ''")
  .filter("created_at is not null and created_at != ''")
  .filter($"lang".isNotNull.and($"lang".isin("ar", "en", "fr", "in", "pt", "es", "tr", "ja")))
  .filter("not (in_reply_to_user_id is not null and in_reply_to_user_id = user.id)")  // filter reply to oneself
  .filter("not (in_reply_to_user_id is null and retweeted_status.user.id = user.id)")  // filter retweet to oneself
  .dropDuplicates("id")  // drop duplicated tweets (TODO: id = null but id_str != null)
  .select($"id".as("tweet_id"), $"created_at", $"text",
  $"user.id".as("user_id"), $"user.screen_name".as("user_name"), $"user.description".as("user_description"), $"user.created_at".as("user_created_at"),
  $"retweeted_status.id".as("retweet_id"), $"retweeted_status.user.id".as("retweet_user_id"),
  $"retweeted_status.user.screen_name".as("retweet_user_screen_name"), $"retweeted_status.user.description".as("retweet_user_desc"),
  $"retweeted_status.user.created_at".as("retweet_user_created_at"),
  $"in_reply_to_user_id".as("reply_user_id"))
  .withColumn("created_at", to_timestamp($"created_at", "EEE MMM dd HH:mm:ss Z yyyy"))  // parse timestamp
  .withColumn("user_created_at", to_timestamp($"user_created_at", "EEE MMM dd HH:mm:ss Z yyyy"))
  .withColumn("retweet_user_created_at", to_timestamp($"retweet_user_created_at", "EEE MMM dd HH:mm:ss Z yyyy"))
  .cache()

val usersDF = contactTweetsDF
  .select($"user_id".as("id"), $"user_description".as("description"), $"user_name".as("screen_name"), $"user_created_at".as("created_at"))
  .union(contactTweetsDF.select($"retweet_user_id".as("id"), $"retweet_user_desc".as("description"), $"retweet_user_screen_name".as("screen_name"), $"retweet_user_created_at".as("created_at")))

// pick the most recent user_name and description
val w = Window.partitionBy($"id").orderBy($"created_at".desc)
val usersUpToDateDF = usersDF
  .filter("(screen_name is not null and screen_name != '') or (description is not null and description != '')")
  .withColumn("rn", row_number.over(w)).where($"rn" === 1)
  .drop("rn")
  .select("id", "description", "screen_name")
  .cache()

usersUpToDateDF.write.save("s3n://cmucc-team-phase1/contact_user_table_v2")

val repliesDF = contactTweetsDF
  .filter("in_reply_to_user_id is not null")
  .select($"user_id".as("user1_id"), $"reply_user_id".as("user2_id"), $"text".as("tweet_text"), $"created_at")

val retweetsDF = contactTweetsDF
  .filter("in_reply_to_user_id is null")
  .select($"user_id".as("user1_id"), $"retweet_user_id".as("user2_id"), $"text".as("tweet_text"), $"created_at")

val contactTweetTextsDF = repliesDF.union(retweetsDF)

case class ContactTweet(user1_id: Long, user2_id: Long, tweet_text: String, created_at: java.sql.Timestamp)

val retweetCounts =  retweetsDF.as[ContactTweet].rdd
  .filter{case ContactTweet(user1ID, user2ID, text, createdAt) => user1ID != user2ID}
  .map{case ContactTweet(user1ID, user2ID, text, createdAt) => ((math.min(user1ID, user2ID), math.max(user1ID, user2ID)), 1)}  // retweet counts as 1
  .reduceByKey(_+_)

val replyCounts = repliesDF.as[ContactTweet].rdd
  .filter{case ContactTweet(user1ID, user2ID, text, createdAt) => user1ID != user2ID}
  .map{case ContactTweet(user1ID, user2ID, text, createdAt) => ((math.min(user1ID, user2ID), math.max(user1ID, user2ID)), 2)}  // reply counts as 2
  .reduceByKey(_+_)

val contactTweetTextIntimacyDF = contactTweetTextsDF.join(intimacyDF, List("user1_id", "user2_id"))
  .dropDuplicates("user1_id", "user2_id", "tweet_text")
  .cache()

contactTweetTextIntimacyDF.write.parquet("s3n://cmucc-team-phase1/contact_tweet_table_v2")
/* this is a Zeppelin notebook
 * this notebook extracts contact_user and contact_tweet
 * table on S3, joins them, and stores the result in JSON for HBase bulk loading
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import spark.implicits._

import org.apache.spark.sql.functions.{row_number, max, broadcast}
import org.apache.spark.sql.expressions.Window

import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client._

sc.hadoopConfiguration.set("fs.s3a.access.key", s3_access_key)
sc.hadoopConfiguration.set("fs.s3a.secret.key",  s3_secret_key)

val usersDF = spark.read.parquet("s3n://cmucc-team-phase1/contact_user_table")
val tweetsDF = spark.read.parquet("s3n://cmucc-team-phase1/contact_tweet_table")

val tweetRepeat = tweetsDF
  .union(tweetsDF.select($"user2_id".as("user1_id"), $"user1_id".as("user2_id"), $"tweet_text", $"created_at", $"intimacy_score"))

val joinedDF = tweetRepeat.join(usersDF, usersDF.col("id") === tweetRepeat.col("user1_id"), "left")
  .drop("id")
  .withColumnRenamed("description", "user1_desc")
  .withColumnRenamed("screen_name", "user1_screen_name")
  .join(usersDF,  usersDF.col("id") === tweetRepeat.col("user2_id"), "left")
  .drop("id")
  .withColumnRenamed("description", "user2_desc")
  .withColumnRenamed("screen_name", "user2_screen_name")

joinedDF.write.json("s3n://cmucc-team-phase1/hbase_table_json")
/**
  * ETL for query 3, range query and topic words extraction.
  * Saves a DF containing censored tweets and impact scores.
  */

package cmu.cc.team.spongebob.spark_etl

import java.util.regex.Pattern

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_timestamp, when}

object TopicWords {
  case class TweetRow(tweet_id: Long, user_id:Long, created_at: Long, text: String,
                      followers_count:Long, retweet_count:Long, favorite_count:Long)

  case class CensoredTweetRow(tweet_id: Long, user_id:Long, created_at: Long, text: String,
                              censored_text: String, impact_score: Long)

  def main(args: Array[String]): Unit = {
    val awsAccessID = args(0)
    val awsSecretAccessKey = args(1)
    val inputFilepath = args(2)
    val inputFilename = args(3)
    val outputFilepath = args(4)
    val outputFilenameSuffix = if (args.length == 6) s"_${args(5)}" else ""

    // create Spark session
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    // set aws credentials
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccessID)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

    // load stop words and censored words
    val stopwords = sc.textFile("s3n://cmucc-datasets/15619/f18/stopwords.txt").collect()
    val censoredWordsEncoded = sc.textFile("s3n://cmucc-datasets/15619/f18/go_flux_yourself.txt")
      .collect().filter(_.nonEmpty)
    val censoredWords = censoredWordsEncoded.map(_.toList.map(decodeOneChar).mkString)

    // load and filter tweets
    val tweetsDF = spark.read
      .option("mode", "DROPMALFORMED")
      .json(s"$inputFilepath/$inputFilename")
    val tweetsFilteredDF = tweetsDF
      .filter("id is not null")
      .filter("user.id is not null or user.id_str is not null")
      .withColumn("user.id",
        when($"user.id".isNull, $"user.id_str".cast("long")).otherwise($"user.id")) // convert id_str to id
      .withColumn("retweet_count",
      when($"retweet_count".isNull, 0).otherwise($"retweet_count"))
      .withColumn("favorite_count",
        when($"favorite_count".isNull, 0).otherwise($"favorite_count"))
      .withColumn("user.followers_count",
        when($"user.followers_count".isNull, 0).otherwise($"user.followers_count"))
      .filter("text is not null and text != ''")
      .filter("created_at is not null and created_at != ''")
      .filter($"lang" === "en")
      .dropDuplicates("id")
      .select($"id".as("tweet_id"), $"user.id".as("user_id"), $"created_at", $"text",
      $"user.followers_count".as("followers_count"),
      $"retweet_count", $"favorite_count")
      .withColumn("created_at", to_timestamp($"created_at", "EEE MMM dd HH:mm:ss Z yyyy").cast("long"))

    /**
      * Compute the impact score of a tweet.
      */
    def computeImpactScore(t: TweetRow) = {
      val pat = Pattern.compile("([A-Za-z0-9'-]*[a-zA-Z][A-Za-z0-9'-]*)")
      // remove short url
      var text = t.text.replaceAll("(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*", "")
      var ewc = 0
      val mat = pat.matcher(text)
      while (mat.find()) {
        val token = mat.group(1)
        if (!stopwords.contains(token.toLowerCase)) {
          ewc += 1
        }
      }
      val impactScore = ewc * (t.followers_count + t.retweet_count + t.favorite_count)
      (t, impactScore)
    }

    /**
      * Censor Tweet text.
      */
    def censorText(t: TweetRow, impactScore: Long) = {
      val pat = Pattern.compile("[A-Za-z0-9]+")
      val mat = pat.matcher(t.text)
      var censored = new String(t.text)
      while (mat.find()) {
        val token = mat.group(0)
        if (censoredWords.contains(token.toLowerCase)) {
          val censoredWord = token.charAt(0) + ("*" * (token.length - 2)) + token.charAt(token.length - 1)
          censored = censored.substring(0, mat.start()) + censoredWord + censored.substring(mat.end())
        }
      }
      CensoredTweetRow(t.tweet_id, t.user_id, t.created_at, t.text, censored, impactScore)
    }

    // compute impact score and censored tweet text
    val impactScoreCensoredRDD = tweetsFilteredDF.as[TweetRow].rdd.
      map(computeImpactScore).
      map{ case (t, impactScore) => censorText(t, impactScore) }

    val impactScoreCensoredDF = impactScoreCensoredRDD.toDF
    impactScoreCensoredDF.write.parquet(s"$outputFilepath/topic_words_parquet$outputFilenameSuffix")
  }

  /**
    * Decode one character using ROT13.
    */
  private def decodeOneChar(c:Char) = {
    if ('a' <= c && c <= 'z') {
      ('a' + (c - 'a' + 13) % 26).toChar
    } else if ('A' <= c && c <= 'Z') {
      ('A' + (c - 'A' + 13) % 26).toChar
    } else {
      c
    }
  }
}

package cmu.cc.team.spongebob.spark_etl.phase2

import java.util.regex.Pattern

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object TopicWord {
  private val s3_access_key = sys.env("aws_access_key")
  private val s3_secret_key = sys.env("aws_secret_key")

  private val sc = new SparkContext()
  private val spark = new SparkSession()
  import spark.implicits._

  sc.hadoopConfiguration.set("fs.s3a.access.key", s3_access_key)
  sc.hadoopConfiguration.set("fs.s3a.secret.key",  s3_secret_key)

  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3_access_key)
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3_secret_key)

  case class TweetRow(tweet_id: Long, user_id:Long, created_at: Long, text: String,
                      followers_count:Long, retweet_count:Long, favorite_count:Long)
  case class CensoredTweetRow(tweet_id: Long, user_id:Long, created_at: Long, text: String,
                              censored_text: String, impact_score: Long)


  def main(args: Array[String]): Unit = {
    val inputFilepath = args(0)
    val outputFilepath = args(1)
    val outputFilenameSuffix = args(2)

    val stopwords = sc.textFile("s3n://cmucc-datasets/15619/f18/stopwords.txt").collect()
    val censoredWordsEncoded = sc.textFile("s3n://cmucc-datasets/15619/f18/go_flux_yourself.txt")
      .collect().filter(_.nonEmpty)
    val censoredWords = censoredWordsEncoded.map(_.toList.map(decodeOneChar).mkString)

    val tweetsDF = spark.read
      .option("mode", "DROPMALFORMED")
      .json(inputFilepath)

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
      .cache()

    val impactScoreCensoredRDD = tweetsFilteredDF.as[TweetRow].rdd
      .map(t => {
        val pat = Pattern.compile("([A-Za-z0-9'-]*[a-zA-Z][A-Za-z0-9'-]*)")
        var text = t.text.replaceAll("(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*", "") // remove short url
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
      })
      .map { case (t, impactScore) =>
        val pat = Pattern.compile("[A-Za-z0-9]+")
        val mat = pat.matcher(t.text)
        var censored = new String(t.text)
        while (mat.find()) {
          val token = mat.group(0)
          if (censoredWords.contains(token.toLowerCase)) {
            var censoredWord = token.charAt(0) + ("*" * (token.length - 2)) + token.charAt(token.length - 1)
            censored = censored.substring(0, mat.start()) + censoredWord + censored.substring(mat.end())
          }
        }
        CensoredTweetRow(t.tweet_id, t.user_id, t.created_at, t.text, censored, impactScore)
      }

    val impactScoreCensoredDF = impactScoreCensoredRDD.toDF
    impactScoreCensoredDF.write.json(s"$outputFilepath/topic_word_json_$outputFilenameSuffix")
  }

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

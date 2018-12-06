package cmu.cc.team.spongebob.spark_etl

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.parsing.json.JSONArray


object LoadUserIntimacyTable {
  """
    |root
    | |-- user1_id: long (nullable = true)
    |w |-- user2_id: long (nullable = true)
    | |-- tweet_text: string (nullable = true)
    | |-- created_at: long (nullable = true)
    | |-- intimacy_score: double (nullable = true)
    | |-- user2_desc: string (nullable = true)
    | |-- user2_screen_name: string (nullable = true)
  """
  case class ContactTweet(user1_id: Long, user2_id: Long, tweet_text: String, created_at: Long, intimacy_score: Double, user2_desc: String, user2_screen_name: String)
  case class GroupedContactTweet(user1_id: Long, user2_ids: String, texts: String, user2_screen_names: String, user2_descs: String, intimacy_scores: String, created_ats: String)

  def main(args: Array[String]): Unit = {
    val jdbcHostname = args(0)
    val topicWordsPath = args(1)
    val userIntimacyPath = args(2)

    // TODO put credentials in a config file
    val jdbcPort = "3306"
    val jdbcDatabase = "twitter_analytics"
    val jdbcUsername = "*****"
    val jdbcPassword = "*****"

    val jdbcUrl =s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase"

    val driverClass = "com.mysql.jdbc.Driver"
    Class.forName(driverClass)  // check jdbc driver

    // test connections
    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
    assert(!connection.isClosed)

    // create statement
    val stmt = connection.createStatement()

    // set connection properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"$jdbcUsername")
    connectionProperties.put("password", s"$jdbcPassword")
    connectionProperties.setProperty("Driver", driverClass)
    connectionProperties.setProperty("characterEncoding", "UTF8")
    connectionProperties.setProperty("charSet", "utf8mb4")
    connectionProperties.setProperty("useUnicode", "true")

    connectionProperties.setProperty("useServerPrepStmts", "false")
    connectionProperties.setProperty("rewriteBatchedStatements", "true")


    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val contactUsersDF = spark.read.parquet("gs://user-intimacy/contact_user_parquet").cache
    val contactTweetsDF = spark.read.parquet("gs://user-intimacy/contact_tweet_parquet").cache

    val contactTweetsRepeatedDF = contactTweetsDF.
      union(contactTweetsDF.select($"user2_id".as("user1_id"), $"user1_id".as("user2_id"), $"tweet_text", $"created_at", $"intimacy_score"))

    val userIntimacyDF = contactTweetsRepeatedDF.
      join(contactUsersDF,  contactTweetsRepeatedDF.col("user2_id") === contactUsersDF.col("id"), "left").
      drop("id").
      withColumnRenamed("description", "user2_desc").
      withColumnRenamed("screen_name", "user2_screen_name")

    val groupedDF = userIntimacyDF.as[ContactTweet].rdd.
      map(c => (c.user1_id, c)).
      groupByKey().
      map { case (user1_id, contactTweets) =>
        val grouped = contactTweets.groupBy(_.user2_id)
        val user2Ids = JSONArray(grouped.keys.toList)
        val user2ScreenNames = JSONArray(grouped.values.map(ts => ts.head.user2_screen_name).map(sn => if (sn == null) "$NULL$" else sn).toList)
        val user2Descs = JSONArray(grouped.values.map(ts => ts.head.user2_desc).map(d => if (d == null) "$NULL$" else d).toList)
        val intimacyScores = JSONArray(grouped.values.map(ts => ts.head.intimacy_score).toList)
        val texts = JSONArray(grouped.values.map(ts => JSONArray(ts.map(_.tweet_text).toList)).toList)
        val createdAts = JSONArray(grouped.values.map(ts => JSONArray(ts.map(_.created_at).toList)).toList)
        GroupedContactTweet(user1_id, user2Ids.toString, texts.toString, user2ScreenNames.toString, user2Descs.toString, intimacyScores.toString, createdAts.toString)
      }.toDF

    // create table
    stmt.executeUpdate("drop table if exists user_intimacy")
    stmt.executeUpdate("create table user_intimacy (user1_id bigint, user2_ids mediumtext, texts mediumtext, created_ats mediumtext, intimacy_scores mediumtext, user2_descs mediumtext, user2_screen_names mediumtext, primary key (user1_id))")

    groupedDF.write.mode(SaveMode.Append)
      .option("charSet", "utf8mb4")
      .option("useUnicode", "true")
      .jdbc(jdbcUrl, "user_intimacy", connectionProperties)

    stmt.close()
    connection.close()
  }
}

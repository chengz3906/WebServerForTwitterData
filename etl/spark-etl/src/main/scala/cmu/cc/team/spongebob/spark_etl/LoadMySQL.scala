package cmu.cc.team.spongebob.spark_etl

import org.apache.spark.sql.{SaveMode, SparkSession}
import java.sql.DriverManager
import java.util.Properties


object LoadMySQL {
  def main(args: Array[String]): Unit = {
    val jdbcHostname = args(0)
    val jdbcPort = "3306"
    val jdbcDatabase = "twitter_analytics"
    val jdbcUsername = "spongebob"
    val jdbcPassword = "15619phase3"

    val jdbcUrl =s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase"

    val driverClass = "com.mysql.cj.jdbc.Driver"
    Class.forName(driverClass)  // check jdbc driver

    // test connections
    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
    assert(!connection.isClosed)

    // create statement
    val stmt = connection.createStatement()

    // create schema
    stmt.executeUpdate("drop table if exists topic_word")
    stmt.executeUpdate("create table topic_word (tweet_id bigint, user_id bigint, created_at bigint, text text, censored_text text, impact_score bigint)")

    stmt.executeUpdate("drop table if exists user_intimacy")
    stmt.executeUpdate("create table user_intimacy (user1_id bigint, user2_id bigint, tweet_text text, created_at bigint, intimacy_score double, user1_desc text, user2_desc text, user1_screen_name text, user2_screen_name text)")

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

    // load tables
    val spark = SparkSession.builder().getOrCreate()
    val topicWordsDF = spark.read.parquet("gs://topic-words/topic_words_parquet_part0")
    val userIntmacyDF = spark.read.parquet("gs://user-intimacy/user_intimacy_parquet_part0")
    topicWordsDF.write.mode(SaveMode.Append)
      .option("charSet", "utf8mb4")
      .option("useUnicode", "true")
      .jdbc(jdbcUrl, "topic_word", connectionProperties)
    userIntmacyDF.write.mode(SaveMode.Append)
      .option("charSet", "utf8mb4")
      .option("useUnicode", "true")
      .jdbc(jdbcUrl, "user_intimacy", connectionProperties)

    // create indexes
    stmt.executeUpdate("create index user_id_created_at_ind on topic_word (user_id, created_at)")
    stmt.executeUpdate("create index user1_id_ind  on user_intimacy (user1_id)")
    stmt.executeUpdate("create index user2_id_ind  on user_intimacy (user2_id)")

    stmt.close()
    connection.close()
  }
}

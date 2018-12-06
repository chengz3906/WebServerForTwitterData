package cmu.cc.team.spongebob.spark_etl

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}


object LoadTopicWordTable {
  def main(args: Array[String]): Unit = {
    val jdbcHostname = args(0)
    val topicWordsPath = args(1)

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

    // create schema
    stmt.executeUpdate("drop table if exists topic_word")
    stmt.executeUpdate("create table topic_word (tweet_id bigint, user_id bigint, created_at bigint, text text, impact_score bigint, _gid int, primary key (user_id, created_at, _gid))")

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
    val topicWordsDF = spark.read.parquet(topicWordsPath).drop("censored_text")

    topicWordsDF.write.mode(SaveMode.Append)
      .option("charSet", "utf8mb4")
      .option("useUnicode", "true")
      .jdbc(jdbcUrl, "topic_word", connectionProperties)

    stmt.close()
    connection.close()
  }
}

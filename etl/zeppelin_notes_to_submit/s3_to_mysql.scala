/* this is a Zeppelin notebook
 * this notebook extracts contact_user and contact_tweet in S3
 * and loads the tables to MySQL
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import spark.implicits._

import org.apache.spark.sql.functions.{row_number, max, broadcast}
import org.apache.spark.sql.expressions.Window

import java.sql.DriverManager
import java.util.Properties


val jdbcUrl =s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

val driverClass = "com.mysql.jdbc.Driver"
Class.forName(driverClass)  // check jdbc driver

val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
connection.isClosed()

// sql statement
val stmt = connection.createStatement()

val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
connectionProperties.setProperty("Driver", driverClass)
connectionProperties.setProperty("characterEncoding", "UTF8")
connectionProperties.setProperty("charSet", "utf8mb4")
connectionProperties.setProperty("useUnicode", "true")

connectionProperties.setProperty("useServerPrepStmts", "false")
connectionProperties.setProperty("rewriteBatchedStatements", "true")

sc.hadoopConfiguration.set("fs.s3a.access.key", s3_access_key)
sc.hadoopConfiguration.set("fs.s3a.secret.key",  s3_secret_key)

spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3_access_key)
spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3_secret_key)

val usersUpToDateDF = spark.read.parquet("s3n://cmucc-team-phase1/contact_user_table")

var rs = stmt.executeUpdate("drop table if exists contact_user")

usersUpToDateDF
  .write
  .option("charSet", "utf8mb4")
  .option("useUnicode", "true")
  .jdbc(jdbcUrl, "contact_user", connectionProperties)

rs = stmt.executeUpdate("create index id_ind on contact_user (id)")

val contactTweetTextIntimacyDF = spark.read.parquet("s3n://cmucc-team-phase1/contact_tweet_table")

var rs = stmt.executeUpdate("drop table if exists contact_tweet")

contactTweetTextIntimacyDF
  .dropDuplicates("user1_id", "user2_id", "tweet_text")
  .write
  .option("charSet", "utf8mb4")
  .option("useUnicode", "true")
  .jdbc(jdbcUrl, "contact_tweet", connectionProperties)

// rs =  stmt.executeUpdate("drop index retweet_user_id_ind on retweet")
rs = stmt.executeUpdate("create index user1_id_ind on contact_tweet (user1_id)")
rs = stmt.executeUpdate("create index user2_id_ind on contact_tweet (user2_id)")
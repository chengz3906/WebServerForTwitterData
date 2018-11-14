package cmu.cc.team.spongebob.spark_etl.phase2

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.SparkSession


object LoadToMySQL {
  private val jdbcHostname =  sys.env("jdbcHostname")
  private val jdbcPort = sys.env("jdbcPort")
  private val jdbcDatabase = sys.env("jdbcDatabase")
  private val jdbcUsername = sys.env("jdbcUsername")
  private val jdbcPassword = sys.env("jdbcPassword")
  private val s3_access_key = sys.env("s3_access_key")
  private val s3_secret_key = sys.env("s3_secret_key")

  private val jdbcUrl =s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase"

  private val driverClass = "com.mysql.jdbc.Driver"
  Class.forName(driverClass)  // check jdbc driver

  private val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
  assert(connection.isClosed)

  private val spark = new SparkSession()
  import spark.implicits._
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3_access_key)
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3_secret_key)

  def main(args: Array[String]): Unit = {
    val inputFilepath = args(0)
    val tableName = args(1)

    val impactScoreCensoredDF = spark.read.json(inputFilepath)

    val stmt = connection.createStatement()
    stmt.executeUpdate(s"drop $tableName if exists topic_word")

    impactScoreCensoredDF
      .select($"censored_text", $"created_at", $"impact_score", $"text", $"tweet_id", $"user_id")
      .write
      .option("charSet", "utf8mb4")
      .option("useUnicode", "true")
      .jdbc(jdbcUrl, tableName, getJDBCConnProperties)

    stmt.executeUpdate(s"create index user_id_created_at_ind on $tableName (user_id, created_at)")
  }

  private def getJDBCConnProperties =  {
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"$jdbcUsername")
    connectionProperties.put("password", s"$jdbcPassword")
    connectionProperties.setProperty("Driver", driverClass)
    connectionProperties.setProperty("characterEncoding", "UTF8")
    connectionProperties.setProperty("charSet", "utf8mb4")
    connectionProperties.setProperty("useUnicode", "true")

    connectionProperties.setProperty("useServerPrepStmts", "false")
    connectionProperties.setProperty("rewriteBatchedStatements", "true")

    connectionProperties
  }
}

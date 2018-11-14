package cmu.cc.team.spongebob.spark_etl.phase1

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
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3_access_key)
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3_secret_key)

  def main(args: Array[String]): Unit = {
    val inputFilepath = args(0)
    val tableName = args(1)

    val contactTweetDF = spark.read.parquet(inputFilepath)

    val stmt = connection.createStatement
    var rs = stmt.executeUpdate(s"drop table if exists $tableName")

    contactTweetDF
      .write
      .option("charSet", "utf8mb4")
      .option("useUnicode", "true")
      .jdbc(jdbcUrl, tableName, getJDBCConnProperties)

    rs = stmt.executeUpdate(s"create index user_id_ind on $tableName (user1_id)")
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

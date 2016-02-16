package com.ignition

import scala.concurrent.blocking

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

import com.ignition.util.ConfigUtils

/**
 * Spark test helper.
 *
 * @author Vlad Orzhekhovskiy
 */
trait SparkTestHelper extends BeforeAllAfterAll {

  private val config = ConfigUtils.getConfig("spark.test")

  protected def sparkConf = new SparkConf(true).
    set("spark.sql.retainGroupColumns", "false").
    set("spark.app.id", "ignition-test")
  //    set("spark.cassandra.connection.host", CassandraBaseTestHelper.host).
  //    set("spark.cassandra.connection.native.port", CassandraBaseTestHelper.port.toString).
  //    set("spark.cassandra.connection.rpc.port", CassandraBaseTestHelper.thriftPort.toString)

  val masterUrl = config.getString("master-url")
  val appName = config.getString("app-name")

  protected def createSparkContext: SparkContext =
    new SparkContext(masterUrl, appName, sparkConf)

  protected def createSQLContxt(sc: SparkContext): SQLContext = new SQLContext(sc)

  protected def clearContext(sc: SparkContext) = blocking {
    sc.stop
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.master.port")
  }
}
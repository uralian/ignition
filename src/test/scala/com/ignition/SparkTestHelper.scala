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
    set("spark.cassandra.connection.host", CassandraBaseTestHelper.host).
    set("spark.cassandra.connection.native.port", CassandraBaseTestHelper.port.toString).
    set("spark.cassandra.connection.rpc.port", CassandraBaseTestHelper.thriftPort.toString)

  val masterUrl = config.getString("master-url")
  val appName = config.getString("app-name")
  implicit protected val sc = new SparkContext(masterUrl, appName, sparkConf)

  implicit protected val ctx = new SQLContext(sc)
  import ctx.implicits._

  implicit protected val rt = new SparkRuntime(ctx)

  protected def clearContext = blocking {
    sc.stop
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.master.port")
  }

  override def afterAll = clearContext
}
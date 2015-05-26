package com.ignition

import scala.concurrent.blocking
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import com.ignition.util.ConfigUtils
import org.apache.spark.streaming.StreamingContext
import java.util.concurrent.TimeUnit
import org.apache.spark.streaming.Milliseconds

/**
 * Spark test helper.
 *
 * @author Vlad Orzhekhovskiy
 */
trait SparkTestHelper extends BeforeAllAfterAll {

  protected def startStreaming: Boolean = false

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

  implicit protected val ssc = {
    val cfg = config.getConfig("streaming")
    val ms = cfg.getDuration("batch-duration", TimeUnit.MILLISECONDS)
    val ssc = new StreamingContext(sc, Milliseconds(ms))
    val checkpointDir = cfg.getString("checkpoint-dir")
    ssc.checkpoint(checkpointDir)
    ssc
  }

  implicit protected val rt = new DefaultSparkRuntime(ctx, ssc)

  protected def clearContext = blocking {
    if (startStreaming) {
      val ms = config.getDuration("streaming.termination-timeout", TimeUnit.MILLISECONDS)
      ssc.stop(false, true)
      ssc.awaitTerminationOrTimeout(ms)
    }
    sc.stop
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.master.port")
  }

  override def beforeAll = if (startStreaming) ssc.start

  override def afterAll = clearContext
}
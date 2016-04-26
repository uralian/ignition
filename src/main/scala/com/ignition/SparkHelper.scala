package com.ignition

import scala.collection.JavaConverters.asScalaSetConverter
import scala.util.control.NonFatal

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{ Duration, StreamingContext }
import org.slf4j.LoggerFactory

import com.ignition.util.ConfigUtils
import com.typesafe.config.ConfigValueType.{ BOOLEAN, NUMBER, STRING }

/**
 * Encapsulates functions for accessing Spark.
 */
object SparkHelper {
  import com.ignition.util.ConfigUtils._

  private val log = LoggerFactory.getLogger(getClass)

  /* build spark configuration */
  val sparkConf = {
    val conf = new SparkConf(true)

    // set main configuration options
    val sparkCfg = getConfig("spark")
    conf.setMaster(sparkCfg.getString("master-url"))
    conf.setAppName(sparkCfg.getString("app-name"))
    conf.set("spark.app.id", sparkCfg.getString("app-name"))

    // set any additional options found
    val properties = sparkCfg.entrySet.asScala
    conf setAll properties.map { entry =>
      val key = entry.getKey
      val value = entry.getValue
      val strValue = value.valueType match {
        case NUMBER  => sparkCfg.getNumber(key).toString
        case BOOLEAN => sparkCfg.getBoolean(key).toString
        case STRING  => sparkCfg.getString(key)
        case _       => throw new IllegalArgumentException(s"Invalid configuration value: $value")
      }
      ("spark." + key, strValue)
    }

    // set cassandra connector options
    val cassCfg = getConfig("cassandra")
    cassCfg.getStringOption("host") foreach (conf.set("spark.cassandra.connection.host", _))
    cassCfg.getStringOption("port") foreach (conf.set("spark.cassandra.connection.port", _))
    cassCfg.getStringOption("username") foreach (conf.set("spark.cassandra.auth.username", _))
    cassCfg.getStringOption("password") foreach (conf.set("spark.cassandra.auth.password", _))

    conf
  }

  /* lazily create Spark Context and SQL Context */
  lazy val sparkContext = new SparkContext(sparkConf)
  lazy val sqlContext = new HiveContext(sparkContext)

  /* read additional configuration settings */
  private val streamCfg = getConfig("spark.streaming")
  val checkpointDir = streamCfg.getString("checkpoint-dir")
  val terminationTimeout = streamCfg.getTimeInterval("termination-timeout")

  /**
   * Creates a streaming context.
   */
  def createStreamingContext(batchDuration: Duration) =
    new StreamingContext(sparkContext, batchDuration) having (_.checkpoint(checkpointDir))

  /**
   * Stops a streaming context.
   *
   * First it tries to stop it gracefully and waits for it to shut down. If that does not
   * happen within `terminationTimeout`, it forces the context to close.
   */
  def stop(ssc: StreamingContext): Unit = {
    ssc.stop(false, true)
    try {
      if (!ssc.awaitTerminationOrTimeout(terminationTimeout.getMillis)) {
        log.warn("Graceful termination failed, forcing stop streaming")
        ssc.stop(false, false)
        ssc.awaitTermination
      }
      log.info("Streaming Context stopped")
    } catch {
      case NonFatal(e) => log.warn("Error stopping StreamingContext", e)
    }
  }

  /**
   * Safely closes the spark context and clear the ports.
   */
  def shutdown() = {
    sparkContext.stop
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.master.port")
  }
}
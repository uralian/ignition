package com.ignition

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import com.ignition.flow.DataFlow
import com.ignition.util.ConfigUtils

/**
 * The entry point for starting ignition flows.
 *
 * @author Vlad Orzhekhovskiy
 */
object SparkPlug {
  import ConfigUtils._

  private val config = ConfigUtils.getConfig("spark")

  private val sparkConf = {
    val conf = new SparkConf(true)

    val cassCfg = ConfigUtils.getConfig("cassandra")
    cassCfg.getStringOption("host") foreach (conf.set("spark.cassandra.connection.host", _))
    cassCfg.getStringOption("port") foreach (conf.set("spark.cassandra.connection.native.port", _))
    cassCfg.getStringOption("thrift-port") foreach (conf.set("spark.cassandra.connection.rpc.port", _))
    cassCfg.getStringOption("username") foreach (conf.set("spark.cassandra.auth.username", _))
    cassCfg.getStringOption("password") foreach (conf.set("spark.cassandra.auth.password", _))

    val sparkCfg = ConfigUtils.getConfig("spark")
    conf.setMaster(sparkCfg.getString("master-url"))
    conf.setAppName(sparkCfg.getString("app-name"))

    conf
  }

  implicit protected lazy val sc = new SparkContext(sparkConf)
  implicit protected lazy val ctx = new SQLContext(sc)
  implicit protected lazy val ssc = {
    val streamCfg = ConfigUtils.getConfig("spark.streaming")
    val ms = config.getDuration("batch-duration", TimeUnit.MILLISECONDS)
    val ssc = new StreamingContext(sc, Milliseconds(ms))
    val checkpointDir = config.getString("checkpoint-dir")
    ssc.checkpoint(checkpointDir)
    ssc
  }
  implicit protected lazy val runtime = new DefaultSparkRuntime(ctx, ssc)

  def runDataFlow(flow: DataFlow,
    vars: Map[String, Any] = Map.empty,
    accs: Map[String, Any] = Map.empty,
    args: Array[String] = Array.empty) = {

    vars foreach {
      case (name, value) => runtime.vars(name) = value
    }

    accs foreach {
      case (name, value: Int) => runtime.accs(name) = value
      case (name, value: Long) => runtime.accs(name) = value
      case (name, value: Float) => runtime.accs(name) = value
      case (name, value: Double) => runtime.accs(name) = value
    }

    (args zipWithIndex) foreach {
      case (arg, index) => runtime.vars(s"arg$index") = arg
    }

    flow.execute
  }

  def shutdown() = sc.stop
}
package com.ignition

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

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

  def runDataFlow(flow: DataFlow) = flow.execute

  def shutdown() = sc.stop
}
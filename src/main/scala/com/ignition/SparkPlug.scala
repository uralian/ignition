package com.ignition

import java.io.File
import java.util.concurrent.TimeUnit

import scala.io.Source
import scala.xml.XML

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{ Milliseconds, StreamingContext }
import org.json4s.jackson.JsonMethods.parse
import org.json4s.string2JsonInput

import com.ignition.frame.DataFlow
import com.ignition.stream.{ DefaultSparkStreamingRuntime, StreamFlow }
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
    conf.set("spark.app.id", sparkCfg.getString("app-name"))

    conf
  }

  implicit protected lazy val sc = new SparkContext(sparkConf)
  implicit protected lazy val ctx = new SQLContext(sc)
  implicit protected lazy val ssc = {
    val streamCfg = ConfigUtils.getConfig("spark.streaming")
    val ms = streamCfg.getDuration("batch-duration", TimeUnit.MILLISECONDS)
    val ssc = new StreamingContext(sc, Milliseconds(ms))
    val checkpointDir = streamCfg.getString("checkpoint-dir")
    ssc.checkpoint(checkpointDir)
    ssc
  }
  implicit protected lazy val runtime = new DefaultSparkStreamingRuntime(ctx, ssc)

  def runDataFlow(flow: DataFlow,
                  vars: Map[String, Any] = Map.empty,
                  accs: Map[String, Any] = Map.empty,
                  args: Array[String] = Array.empty) = {

    vars foreach {
      case (name, value) => runtime.vars(name) = value
    }

    accs foreach {
      case (name, value: Int)    => runtime.accs(name) = value
      case (name, value: Long)   => runtime.accs(name) = value
      case (name, value: Float)  => runtime.accs(name) = value
      case (name, value: Double) => runtime.accs(name) = value
    }

    (args zipWithIndex) foreach {
      case (arg, index) => runtime.vars(s"arg$index") = arg
    }

    flow.execute
  }

  def startStreamFlow(flow: StreamFlow,
                      vars: Map[String, Any] = Map.empty,
                      accs: Map[String, Any] = Map.empty,
                      args: Array[String] = Array.empty) = {

    vars foreach {
      case (name, value) => runtime.vars(name) = value
    }

    accs foreach {
      case (name, value: Int)    => runtime.accs(name) = value
      case (name, value: Long)   => runtime.accs(name) = value
      case (name, value: Float)  => runtime.accs(name) = value
      case (name, value: Double) => runtime.accs(name) = value
    }

    (args zipWithIndex) foreach {
      case (arg, index) => runtime.vars(s"arg$index") = arg
    }

    flow.start
  }

  def shutdown() = {
    ssc.stop(false, false)
    sc.stop
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.master.port")
  }

  case class FlowConfig(flowfile: File = null, fileType: String = "json", args: Seq[String] = Seq.empty)

  val parser = new scopt.OptionParser[FlowConfig]("SparkPlug") {
    head("SparkPlug", "0.3")
    note("This program runs a single dataflow.\n")

    opt[String]("type") optional () valueName ("json|xml") action { (x, c) =>
      c.copy(fileType = x)
    } text ("flow file format (default: json)")
    help("help") text ("prints this usage text")
    arg[File]("dataflow") required () action { (x, c) =>
      c.copy(flowfile = x)
    } text ("dataflow file")
    arg[String]("arg0 arg1 ...") unbounded () optional () action { (x, c) =>
      c.copy(args = c.args :+ x)
    } text ("dataflow parameters")

    override def showUsageOnError: Boolean = true
  }

  /**
   * Entry point to start a data flow.
   */
  def main(args: Array[String]): Unit = {
    val cfg = parser.parse(args, FlowConfig()) getOrElse sys.exit

    val data = Source.fromFile(cfg.flowfile).getLines mkString "\n"
    val flow = if (cfg.fileType == "json")
      DataFlow.fromJson(parse(data))
    else
      DataFlow.fromXml(XML.loadString(data))

    runDataFlow(flow = flow, args = args)
  }
}
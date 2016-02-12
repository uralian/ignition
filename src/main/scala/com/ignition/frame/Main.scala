package com.ignition.frame

import java.io.File
import scala.io.Source
import scala.xml.XML
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.json4s.jackson.JsonMethods.parse
import org.json4s.string2JsonInput
import com.ignition.util.ConfigUtils
import com.ignition.BuildInfo

/**
 * The entry point for starting ignition frame flows.
 *
 * @author Vlad Orzhekhovskiy
 */
object Main {
  import ConfigUtils._

  /* build spark configuration */
  private val sparkConf = {
    val conf = new SparkConf(true)

    val cassCfg = ConfigUtils.getConfig("cassandra")
    cassCfg.getStringOption("host") foreach (conf.set("spark.cassandra.connection.host", _))
    cassCfg.getStringOption("port") foreach (conf.set("spark.cassandra.connection.port", _))
    cassCfg.getStringOption("username") foreach (conf.set("spark.cassandra.auth.username", _))
    cassCfg.getStringOption("password") foreach (conf.set("spark.cassandra.auth.password", _))

    val sparkCfg = ConfigUtils.getConfig("spark")
    conf.setMaster(sparkCfg.getString("master-url"))
    conf.setAppName(sparkCfg.getString("app-name"))
    conf.set("spark.app.id", sparkCfg.getString("app-name"))

    conf
  }

  /* constructs spark runtime */
  lazy val sc = new SparkContext(sparkConf)
  lazy val ctx = new SQLContext(sc)
  implicit protected lazy val runtime = new DefaultSparkRuntime(ctx)

  /* build command line parser */
  val parser = new scopt.OptionParser[FrameFlowConfig]("FrameFlowRunner") {
    head(BuildInfo.name, BuildInfo.version)
    note("This program runs a single frameflow.\n")

    opt[String]("type") optional () valueName ("json|xml") action { (x, c) =>
      c.copy(fileType = x)
    } text ("flow file format (default: json)")
    help("help") text ("prints this usage text")
    arg[File]("frameflow") required () action { (x, c) =>
      c.copy(flowfile = x)
    } text ("frameflow file")
    arg[String]("arg0 arg1 ...") unbounded () optional () action { (x, c) =>
      c.copy(args = c.args :+ x)
    } text ("frameflow parameters")

    override def showUsageOnError: Boolean = true
  }

  /**
   * Entry point to start a stream flow.
   */
  def main(args: Array[String]): Unit = {
    val cfg = parser.parse(args, FrameFlowConfig()) getOrElse sys.exit
    val data = Source.fromFile(cfg.flowfile).getLines mkString "\n"
    val flow = if (cfg.fileType == "json")
      FrameFlow.fromJson(parse(data))
    else
      FrameFlow.fromXml(XML.loadString(data))

    runFrameFlow(flow = flow, args = args)
  }

  /**
   * Runs the specified frame flow.
   * @param flow flow to run.
   * @param vars variables to inject before running the flow.
   * @param accs accumulators to inject before running the flow.
   * @param args an array of command line parameters to inject as variables under name 'arg\$index'.
   * @return a list of evaluated flow outputs.
   */
  def runFrameFlow(flow: FrameFlow,
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

  /**
   * FrameFlow configuration.
   */
  case class FrameFlowConfig(flowfile: File = null, fileType: String = "json", args: Seq[String] = Seq.empty)
}
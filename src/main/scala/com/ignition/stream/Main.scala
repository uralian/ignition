package com.ignition.stream

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.future
import scala.io.Source
import scala.xml.XML

import org.apache.spark.streaming.{ Milliseconds, StreamingContext }
import org.json4s.jackson.JsonMethods.parse
import org.json4s.string2JsonInput

import com.ignition.{ BuildInfo, frame }
import com.ignition.util.ConfigUtils

/**
 * The entry point for starting ignition stream flows.
 *
 * @author Vlad Orzhekhovskiy
 */
object Main {
  import ConfigUtils._

  private val flows = collection.mutable.Map.empty[UUID, StreamingContext]

  /* build command line parser */
  val parser = new scopt.OptionParser[StreamFlowConfig]("StreamFlowRunner") {
    head(BuildInfo.name, BuildInfo.version)
    note("This program runs a single frameflow.\n")

    opt[String]("type") optional () valueName ("json|xml") action { (x, c) =>
      c.copy(fileType = x)
    } text ("flow file format (default: json)")
    help("help") text ("prints this usage text")
    arg[File]("streamflow") required () action { (x, c) =>
      c.copy(flowfile = x)
    } text ("streamflow file")
    arg[String]("arg0 arg1 ...") unbounded () optional () action { (x, c) =>
      c.copy(args = c.args :+ x)
    } text ("streamflow parameters")

    override def showUsageOnError: Boolean = true
  }

  /**
   * Entry point to start a data flow.
   */
  def main(args: Array[String]): Unit = {
    val cfg = parser.parse(args, StreamFlowConfig()) getOrElse sys.exit
    val data = Source.fromFile(cfg.flowfile).getLines mkString "\n"
    val flow = if (cfg.fileType == "json")
      StreamFlow.fromJson(parse(data))
    else
      StreamFlow.fromXml(XML.loadString(data))

    startStreamFlow(flow = flow, args = args)
  }

  /**
   * Starts the specified stream flow.
   * @param flow flow to run.
   * @param vars variables to inject before running the flow.
   * @param accs accumulators to inject before running the flow.
   * @param args an array of command line parameters to inject as variables under name 'arg\$index'.
   * @return the newly generated flow id and the future which will complete when the flow terminates.
   */
  def startStreamFlow(flow: StreamFlow,
                      vars: Map[String, Any] = Map.empty,
                      accs: Map[String, Any] = Map.empty,
                      args: Array[String] = Array.empty): (UUID, Future[Unit]) = {

    val ssc = createStreamingContext
    implicit val runtime = new DefaultSparkStreamingRuntime(frame.Main.ctx, ssc)

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

    val f = future(flow.start)

    val id = UUID.randomUUID
    flows += id -> ssc

    id -> f
  }

  /**
   * Starts the specified stream flow and waits for it to complete.
   * @param flow flow to run.
   * @param vars variables to inject before running the flow.
   * @param accs accumulators to inject before running the flow.
   * @param args an array of command line parameters to inject as variables under name 'arg\$index'.
   */
  def startAndWait(flow: StreamFlow,
                   vars: Map[String, Any] = Map.empty,
                   accs: Map[String, Any] = Map.empty,
                   args: Array[String] = Array.empty) = {

    val (id, f) = startStreamFlow(flow, vars, accs, args)
    println(s"Flow #$id started")
    Await.ready(f, Duration.Inf)
  }

  /**
   * Stops the specified workflow.
   */
  def stopStreamFlow(id: UUID) = flows(id).stop(false, true)

  /**
   * Returns the IDs of the running flows.
   */
  def getRunning = flows.keySet

  /**
   * Creates a new streaming context.
   */
  private def createStreamingContext = {
    val streamCfg = ConfigUtils.getConfig("spark.streaming")
    val ms = streamCfg.getDuration("batch-duration", TimeUnit.MILLISECONDS)
    val ssc = new StreamingContext(frame.Main.sc, Milliseconds(ms))
    val checkpointDir = streamCfg.getString("checkpoint-dir")
    ssc.checkpoint(checkpointDir)
    ssc
  }

  /**
   * StreamFlow configuration.
   */
  case class StreamFlowConfig(flowfile: File = null, fileType: String = "json", args: Seq[String] = Seq.empty)
}
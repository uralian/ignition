package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2Assoc, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Executes an SQL statement against the inputs. Each input is injected as a table
 * under the name "inputX" where X is the index of the input.
 *
 * @author Vlad Orzhekhovskiy
 */
case class SQLQuery(query: String) extends FrameMerger(SQLQuery.MAX_INPUTS) {
  import SQLQuery._

  override val allInputsRequired = false

  protected def compute(args: IndexedSeq[DataFrame])(implicit runtime: SparkRuntime): DataFrame = {
    assert(args.exists(_ != null), "No connected inputs")

    args.zipWithIndex foreach {
      case (df, index) if df != null => df.registerTempTable(s"input$index")
      case _ => /* do nothing */
    }

    val query = injectGlobals(this.query)

    val df = ctx.sql(query)
    optLimit(df, runtime.previewMode)
  }

  def toXml: Elem = <node>{ query }</node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("query" -> query)
}

/**
 * SQL query companion object.
 */
object SQLQuery {
  val tag = "sql"

  val MAX_INPUTS = 10

  def fromXml(xml: Node) = SQLQuery(xml.child.head asString)

  def fromJson(json: JValue) = SQLQuery(json \ "query" asString)
}
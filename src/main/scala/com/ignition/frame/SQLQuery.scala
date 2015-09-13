package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition.{ SparkRuntime, XmlExport }
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Executes an SQL statement against the inputs. Each input is injected as a table
 * under the name "inputX" where X is the index of the input.
 *
 * @author Vlad Orzhekhovskiy
 */
case class SQLQuery(query: String) extends FrameMerger(SQLQuery.MAX_INPUTS) with XmlExport {

  override val allInputsRequired = false

  protected def compute(args: Seq[DataFrame], limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    assert(args.exists(_ != null), "No connected inputs")

    args.zipWithIndex foreach {
      case (df, index) if df != null => df.registerTempTable(s"input$index")
      case _ => /* do nothing */
    }

    val query = injectGlobals(this.query)

    val df = ctx.sql(query)
    optLimit(df, limit)
  }

  protected def computeSchema(inSchemas: Seq[StructType])(implicit runtime: SparkRuntime): StructType =
    computedSchema(0)

  def toXml: Elem = <sql>{ query }</sql>
}

/**
 * SQL query companion object.
 */
object SQLQuery {
  val MAX_INPUTS = 10

  def fromXml(xml: Node) = SQLQuery(xml.asString)
}
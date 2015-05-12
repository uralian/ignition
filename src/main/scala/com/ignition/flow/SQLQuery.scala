package com.ignition.flow

import scala.Array.canBuildFrom
import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType

import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Executes an SQL statement against the inputs. Each input is injected as a table
 * under the name "inputX" where X is the index of the input.
 *
 * @author Vlad Orzhekhovskiy
 */
case class SQLQuery(query: String) extends Merger(SQLQuery.MAX_INPUTS) with XmlExport {

  override val allInputsRequired = false

  protected def compute(args: Array[DataFrame], limit: Option[Int])(implicit ctx: SQLContext): DataFrame = {
    assert(args.exists(_ != null), "No connected inputs")

    args.zipWithIndex foreach {
      case (df, index) if df != null => df.registerTempTable(s"input$index")
      case _ => /* do nothing */
    }

    val df = ctx.sql(query)
    optLimit(df, limit)
  }
  
  protected def computeSchema(inSchemas: Array[StructType])(implicit ctx: SQLContext): StructType = {
    val df = compute(inputs(Some(1)), Some(1))(ctx)
    df.schema
  }

  def toXml: Elem = <sql>{ query }</sql>

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * SQL query companion object.
 */
object SQLQuery {
  val MAX_INPUTS = 10

  def fromXml(xml: Node) = SQLQuery(xml.asString)
}
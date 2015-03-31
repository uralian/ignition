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

  var schema: Option[StructType] = None

  protected def compute(args: Array[DataFrame])(implicit ctx: SQLContext): DataFrame = {
    assert(args.exists(_ != null), "No connected inputs")

    args.zipWithIndex foreach {
      case (df, index) if df != null => df.registerTempTable(s"input$index")
      case _ => /* do nothing */
    }

    val df = ctx.sql(query)
    this.schema = Some(df.schema)
    df
  }

  protected def computeSchema(inSchemas: Array[Option[StructType]])(implicit ctx: SQLContext) = schema orElse {
    compute(inputs)(ctx)
    schema
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